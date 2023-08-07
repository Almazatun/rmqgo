package rmqgo

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Rmq struct {
	connection        *amqp.Connection
	channel           *amqp.Channel
	replyQueue        *amqp.Queue
	replyQueueName    *string
	topicQueue        *amqp.Queue
	msgChan           chan []byte
	isConnected       bool
	isInitializedRpc  bool
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

type exchanges struct {
	RmqDirect  string
	RmqTopic   string
	RmqFanout  string
	RmqHeaders string
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

var Exchanges = exchanges{
	RmqDirect:  "rmq.direct",
	RmqTopic:   "rmq.topic",
	RmqFanout:  "rmq.fanout",
	RmqHeaders: "rmq.headers",
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

type RmqOption func(*Rmq)

func New(options ...RmqOption) *Rmq {
	rmq := &Rmq{
		connection:        nil,
		channel:           nil,
		replyQueue:        nil,
		replyQueueName:    nil,
		topicQueue:        nil,
		isConnected:       false,
		isInitializedRpc:  false,
		correlationIdsMap: make(map[string]string),
		msgChan:           make(chan []byte),
	}

	for _, opt := range options {
		opt(rmq)
	}

	return rmq
}

func (rmq *Rmq) Connect(config ConnectConfig) error {
	dt := rmq.fillConnectionConfig(config)

	c, err := amqp.Dial("amqp://" + dt.User + ":" + dt.Pass + "@" + dt.Host + dt.Port)

	if err != nil {
		log.Println(err)
		return err
	}

	rmq.connection = c
	rmq.isConnected = true
	ch, err := rmq.CreateChannel()

	if err != nil {
		log.Println(err)
		return err
	}

	rmq.channel = ch

	if rmq.isInitializedRpc {
		rmq.declareReplayQueue(*rmq.replyQueueName)
	}

	return nil
}

func (rmq *Rmq) CreateQueue(config CreateQueueConfig) (q *amqp.Queue, err error) {
	if rmq.channel == nil {
		errorMsg := "Channel not initialized to create queue"
		return nil, errors.New(errorMsg)
	}

	dt := rmq.fillCreateQueueConfig(config)

	args := amqp.Table{}
	args["x-message-ttl"] = dt.MsgTtl

	cq, err := rmq.channel.QueueDeclare(
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

	ch, err := rmq.connection.Channel()

	if err != nil {
		log.Println(err)
		return nil, err
	}

	return ch, nil
}

func (rmq *Rmq) CreateExchange(config CreateExchangeConfig) error {
	if rmq.channel == nil {
		errorMsg := "Channel not initialized to create exchange"
		return errors.New(errorMsg)
	}

	if config.Name == "" {
		log.Fatal("Exchange name required")
	}

	err := rmq.channel.ExchangeDeclare(
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
	if rmq.channel == nil {
		errorMsg := "Channel not initialized to make able to bind queue"
		return errors.New(errorMsg)
	}

	err := rmq.channel.QueueBind(
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
	if rmq.channel == nil || rmq.connection == nil {
		errorMsg := "Rabbit mq not connected"
		return errors.New(errorMsg)
	}

	err := rmq.channel.Close()

	if err != nil {
		return err
	}
	err = rmq.connection.Close()

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

	err = rmq.channel.PublishWithContext(
		ctx,
		Exchanges.RmqDirect, // exchange
		input.ReplayTo,      // routing key
		false,               // mandatory
		false,               // immediate
		amqp.Publishing{
			CorrelationId: input.CorrelationId,
			ContentType:   "text/plain",
			Body:          []byte(b),
		})

	if err != nil {
		log.Println(err)
	}
}

func (rmq *Rmq) declareReplayQueue(replayQueueName string) {
	var err error
	args := make(map[string]interface{})

	rmq.replyQueue, err = rmq.CreateQueue(CreateQueueConfig{
		Name:         replayQueueName,
		DeleteUnused: false,
		Exclusive:    false,
		NoWait:       false,
		Durable:      true,
		Args:         &args,
		MsgTtl:       nil,
	})

	if err != nil {
		log.Fatal(err)
	}

	err = rmq.CreateExchange(CreateExchangeConfig{
		Name:       Exchanges.RmqDirect,
		Type:       ExchangeType.Direct,
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Args:       &args,
	})

	if err != nil {
		log.Fatal(err)
	}

	err = rmq.BindQueueByExchange(BindQueueByExgConfig{
		rmq.replyQueue.Name,
		rmq.replyQueue.Name,
		Exchanges.RmqDirect,
		false,
		&args,
	})

	if err != nil {
		log.Fatal(err)
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

func WithRpc(replayQueueName string) RmqOption {
	if replayQueueName == "" {
		log.Fatal("Replay queue name required")
	}

	return func(rmq *Rmq) {
		if rmq.replyQueueName == nil {
			rmq.replyQueueName = &replayQueueName
		}

		rmq.isInitializedRpc = true
	}
}
