package rmqgo

import (
	"context"
	"encoding/json"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Rmq struct {
	Connection        *amqp.Connection
	Channel           *amqp.Channel
	replyQueue        *amqp.Queue
	topicQueue        *amqp.Queue
	msgChan           chan []byte
	isConnected       bool
	correlationIdsMap map[string]string
}

type ConnectConfig struct {
	User string
	Pass string
	Host string
	Port string
}

type Msg interface{}

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
	Msg
}

type replayMsg struct {
	Msg           interface{}
	Method        string
	CorrelationId string
	ReplayTo      string
	Exchange      string
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
		correlationIdsMap: make(map[string]string),
		msgChan:           make(chan []byte),
	}
}

func (rmq *Rmq) Connect(config ConnectConfig) error {
	dt := rmq.fillConnectionConfig(config)

	c, err := amqp.Dial("amqp://" + dt.User + ":" + dt.Pass + "@" + dt.Host + dt.Port)

	if err != nil {
		log.Println(err)
		return err
	}

	rmq.Connection = c
	rmq.isConnected = true
	ch, err := rmq.CreateChannel()

	if err != nil {
		log.Println(err)
		return err
	}

	rmq.Channel = ch

	return nil
}

func (rmq *Rmq) CreateQueue(config CreateQueueConfig) (q *amqp.Queue, err error) {
	dt := rmq.fillCreateQueueConfig(config)

	args := amqp.Table{}
	args["x-message-ttl"] = dt.MsgTtl

	cq, err := rmq.Channel.QueueDeclare(
		dt.Name,
		dt.Durable,
		dt.DeleteUnused,
		dt.Exclusive,
		dt.NoWait,
		*dt.Args,
	)

	if err != nil {
		log.Println(err)
		return nil, err
	}

	return &cq, nil
}

func (rmq *Rmq) CreateChannel() (c *amqp.Channel, err error) {
	rmq.checkConnection()

	ch, err := rmq.Connection.Channel()

	if err != nil {
		log.Println(err)
		return nil, err
	}

	return ch, nil
}

func (rmq *Rmq) CreateExchange(config CreateExchangeConfig) error {
	if rmq.Channel == nil {
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
		log.Println(err)
		return err
	}

	return nil
}

func (rmq *Rmq) BindQueueByExchange(config BindQueueByExgConfig) error {
	if rmq.Channel == nil {
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
		log.Println(err)
		return err
	}

	return nil
}

func (rmq *Rmq) Close() error {
	err := rmq.Channel.Close()

	if err != nil {
		return err
	}
	err = rmq.Connection.Close()

	if err != nil {
		return err
	}

	return nil
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
		log.Println(err)
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
