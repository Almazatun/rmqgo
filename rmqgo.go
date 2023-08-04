package rmqgo

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/exec"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Rmq struct {
	Connection        *amqp.Connection
	Channel           *amqp.Channel
	replyQueue        *amqp.Queue
	topicQueue        *amqp.Queue
	messageChan       chan []byte
	isConnected       bool
	isInitialized     bool
	isCreatedConsumer bool
	correlationIdsMap map[string]string
}

type ConnectConfig struct {
	User         string
	Pass         string
	Host         string
	Port         string
	IsInit       bool
	NameQueue    *string
	ExchangeName *string
}

type CreateQueueConfig struct {
	// https://www.rabbitmq.com/ttl.html
	// By default 30_000 millisecond
	MsgTtl       *int
	Name         string //queue name
	DeleteUnused bool   //delete when unused
	Exclusive    bool
	NoWait       bool
	Durable      bool
	Args         *map[string]interface{}
}

type exchangeType struct {
	Direct string
	Topic  string
	Fanout string
}

type SendMsg struct {
	Method string
	Msg    interface{}
}

type replayMsg struct {
	Msg           interface{}
	Method        string
	CorrelationId string
	ReplayTo      string
	Exchange      string
}

type consumerMsg struct {
	Method string
	Msg    interface{}
}

var ExchangeType = exchangeType{
	Direct: "direct",
	Topic:  "topic",
	Fanout: "fanout",
}

type CreateExchangeConfig struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       *map[string]interface{}
}

type CreateConsumerConfig struct {
	QueueName string
	Consumer  string
	AutoAck   bool
	Exclusive bool
	NoWait    bool
	NoLocal   bool
	// Make able to run in other thread
	Wg *sync.WaitGroup
}

type BindQueueByExgConfig struct {
	QueueName    string
	RoutingKey   string
	ExchangeName string
	NoWait       bool
	Args         *map[string]interface{}
}

func New() *Rmq {
	return &Rmq{
		Connection:        nil,
		Channel:           nil,
		replyQueue:        nil,
		topicQueue:        nil,
		isConnected:       false,
		isInitialized:     false,
		isCreatedConsumer: false,
		correlationIdsMap: make(map[string]string),
	}
}

func (rmq *Rmq) Connect(config ConnectConfig) {
	dt := rmq.fillConnectionConfig(config)

	c, err := amqp.Dial("amqp://" + dt.User + ":" + dt.Pass + "@" + dt.Host + dt.Port)

	if err != nil {
		log.Fatal(err)
	}

	rmq.Connection = c
	rmq.isConnected = true
	rmq.Channel = rmq.CreateChannel()
	rmq.isInitialized = true

	if config.IsInit {
		m := make(map[string]interface{})

		rmq.replyQueue = rmq.CreateQueue(CreateQueueConfig{
			Name:         *config.NameQueue,
			DeleteUnused: false,
			Exclusive:    false,
			NoWait:       false,
			Durable:      true,
			Args:         &m,
		})

		rmq.CreateExchange(CreateExchangeConfig{
			Name:       rmq.replyQueue.Name,
			Type:       ExchangeType.Direct,
			Durable:    true,
			AutoDelete: false,
			Internal:   false,
			NoWait:     false,
			Args:       &m,
		})

		rmq.BindQueueByExchange(BindQueueByExgConfig{
			rmq.replyQueue.Name,
			*config.NameQueue,
			*config.NameQueue,
			false,
			&m,
		})
	}
}

func (rmq *Rmq) CreateQueue(config CreateQueueConfig) *amqp.Queue {
	dt := rmq.fillCreateQueueConfig(config)

	args := amqp.Table{}
	args["x-message-ttl"] = dt.MsgTtl

	q, err := rmq.Channel.QueueDeclare(
		dt.Name,
		dt.Durable,
		dt.DeleteUnused,
		dt.Exclusive,
		dt.NoWait,
		*dt.Args,
	)

	if err != nil {
		log.Fatal(err)
	}

	return &q
}

func (rmq *Rmq) Send(ex, rk string, msg interface{}, method string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out, err := json.Marshal(SendMsg{
		Msg:    msg,
		Method: method,
	})

	if err != nil {
		log.Fatal(err)
	}

	err = rmq.Channel.PublishWithContext(ctx,
		ex,    // exchange
		rk,    // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(out),
		})

	if err != nil {
		fmt.Println("Failed to declare an exchange")

		return err
	}

	return nil
}

func (rmq *Rmq) SendReplyMsg(ex, rk string, msg interface{}, method string) (res *[]byte, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out, err := json.Marshal(SendMsg{
		Msg:    msg,
		Method: method,
	})

	if err != nil {
		log.Fatal(err)
	}

	corrIdBytes, err := exec.Command("uuidgen").Output()

	if err != nil {
		log.Fatal(err)
	}

	corrId := string(corrIdBytes)

	rmq.correlationIdsMap[corrId] = corrId

	err = rmq.Channel.PublishWithContext(ctx,
		ex,    // exchange
		rk,    // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			Body:          []byte(out),
			ReplyTo:       rmq.replyQueue.Name,
			CorrelationId: corrId,
		})

	if err != nil {
		fmt.Println("Failed to declare an exchange")

		return nil, err
	}

	for msg := range rmq.messageChan {
		res = &msg
		break
	}

	return res, nil
}

func (rmq *Rmq) CreateConsumer(
	config CreateConsumerConfig,
	funcMap map[string]func([]byte) interface{},
) {
	if config.Wg != nil {
		defer config.Wg.Done()
	}

	var err error
	// var ttl int
	args := amqp.Table{
		// "x-dead-letter-exchange":    "",
		// "x-dead-letter-routing-key": config.QueueName,
		// "x-message-ttl":             int64(ttl),
		// "x-expires":                 int64(ttl),
		// "x-max-priority":            uint8(),
	}

	runnerChan := make(chan bool)

	msgs, err := rmq.Channel.Consume(
		config.QueueName, // queue
		config.Consumer,  // consumer
		config.AutoAck,
		config.Exclusive,
		config.NoLocal,
		config.NoWait,
		args,
	)

	rmq.messageChan = make(chan []byte)

	if err != nil {
		fmt.Println("Error when consuming message", err.Error())
	}

	rmq.isCreatedConsumer = true

	go func() {
		for d := range msgs {
			// fmt.Printf("Received a message: %s", d.Body)
			msg := consumerMsg{}
			err := json.Unmarshal(d.Body, &msg)

			if err != nil {
				fmt.Println(err)
			}

			var resMas interface{}

			if funcMap != nil {
				if msg.Method != "" {
					_, ok := funcMap[msg.Method]

					if ok {
						resMas = funcMap[msg.Method](d.Body)
					}
				}
			}

			_, ok := rmq.correlationIdsMap[d.CorrelationId]

			if d.CorrelationId != "" && msg.Method != "" && resMas != nil && !ok {
				rmq.replay(replayMsg{
					Msg:           resMas,
					Method:        msg.Method,
					CorrelationId: d.CorrelationId,
					ReplayTo:      d.ReplyTo,
					Exchange:      d.Exchange,
				})
			} else {

				if ok {
					delete(rmq.correlationIdsMap, d.CorrelationId)
				}

				go func() {
					rmq.messageChan <- d.Body
				}()
			}
			d.Ack(true)
		}
	}()

	if config.Wg == nil {
		log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
		<-runnerChan
	}
}

func (rmq *Rmq) CreateChannel() *amqp.Channel {
	rmq.checkConnection()

	ch, err := rmq.Connection.Channel()

	if err != nil {
		log.Fatal(err)
	}

	rmq.isInitialized = true

	return ch
}

func (rmq *Rmq) CreateExchange(config CreateExchangeConfig) {
	if !rmq.isInitialized {
		log.Fatal("Channel not initialized to create exchange")
	}

	if config.Name == "" {
		log.Fatal("Exchange name required")
	}

	err := rmq.Channel.ExchangeDeclare(
		config.Name,
		config.Type,
		config.Durable,
		config.AutoDelete,
		config.Internal,
		config.NoWait,
		*config.Args,
	)

	if err != nil {
		log.Fatal(err)
	}
}

func (rmq *Rmq) BindQueueByExchange(config BindQueueByExgConfig) {
	if !rmq.isInitialized {
		log.Fatal("Channel not initialized to bind queue")
	}

	err := rmq.Channel.QueueBind(
		config.QueueName,
		config.RoutingKey,
		config.ExchangeName,
		config.NoWait,
		*config.Args,
	)

	if err != nil {
		log.Fatal(err)
	}
}

func (rmq *Rmq) Close() {
	rmq.Channel.Close()
	rmq.Connection.Close()
}

func (rmq *Rmq) replay(input replayMsg) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	b, err := json.Marshal(SendMsg{Msg: input.Msg, Method: input.Method})

	if err != nil {
		log.Fatal(err)
	}

	err = rmq.Channel.PublishWithContext(
		ctx,
		input.ReplayTo, // exchange
		input.ReplayTo, // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			CorrelationId: input.CorrelationId,
			ContentType:   "text/plain",
			Body:          []byte(b),
		})

	if err != nil {
		fmt.Println(err)
	}
}

func (rmq *Rmq) checkConnection() {
	if !rmq.isConnected {
		log.Fatal("RabbitMQ not connected")
	}
}

func (rmq *Rmq) fillConnectionConfig(cf ConnectConfig) ConnectConfig {
	if cf.Port != "" {
		cf.Port = ":" + cf.Port + "/"
	}

	return cf
}

func (rmq *Rmq) fillCreateQueueConfig(cf CreateQueueConfig) CreateQueueConfig {
	if cf.MsgTtl == nil || *cf.MsgTtl < 0 {
		ttl := 30_000
		cf.MsgTtl = &ttl
	}

	if cf.Name == "" {
		log.Fatal("Queue name required")
	}

	return cf
}
