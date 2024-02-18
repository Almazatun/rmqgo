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
	connection     *amqp.Connection
	channel        *amqp.Channel
	replyQueue     *amqp.Queue
	replyQueueData *replayQueueData
	// TODO
	topicQueue        *amqp.Queue
	msgChan           chan []byte
	replayMsgChan     chan []byte
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

type SendMsg struct {
	Method string
	Msg
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

type replayQueueData struct {
	name         string
	exchangeType string
	rk           string
}

type replayMsg struct {
	Msg           interface{}
	Method        string
	CorrelationId string
	ReplayTo      string
	Exchange      string
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

func New(options ...RmqOption) *Rmq {
	rmq := &Rmq{
		connection:        nil,
		channel:           nil,
		replyQueue:        nil,
		topicQueue:        nil,
		replyQueueData:    nil,
		isConnected:       false,
		isInitializedRpc:  false,
		correlationIdsMap: make(map[string]string),
		msgChan:           make(chan []byte),
		replayMsgChan:     make(chan []byte),
	}

	// init optional settings
	for _, opt := range options {
		opt(rmq)
	}

	return rmq
}

func (rmq *Rmq) Connect(config ConnectConfig) error {
	dt := fillConnectionConfig(config)

	c, err := amqp.Dial("amqp://" + dt.User + ":" + dt.Pass + "@" + dt.Host + dt.Port)

	if err != nil {
		return err
	}

	rmq.connection = c

	rmq.isConnected = true
	ch, err := rmq.CreateChannel()

	if err != nil {
		return err
	}

	rmq.channel = ch

	if rmq.isInitializedRpc {
		rmq.declareReplayQueue(*rmq.replyQueueData)
	}

	return nil
}

func (rmq *Rmq) CreateQueue(config CreateQueueConfig) (q *amqp.Queue, err error) {
	if rmq.channel == nil {
		return nil, errors.New("Channel not initialized to create queue")
	}

	dt := fillCreateQueueConfig(config)

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
		return nil, err
	}

	return &cq, nil
}

func (rmq *Rmq) CreateChannel() (c *amqp.Channel, err error) {
	checkConnection(rmq)

	ch, err := rmq.connection.Channel()

	if err != nil {
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
		return err
	}

	return nil
}

func (rmq *Rmq) Close() error {
	if rmq.channel == nil || rmq.connection == nil {
		errorMsg := "Rabbit mq not connected"
		return errors.New(errorMsg)
	}

	close(rmq.msgChan)
	close(rmq.replayMsgChan)

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
	timeoutDurationTimePubMsg := time.Duration(5)
	ctx, cancel := context.WithTimeout(context.Background(), timeoutDurationTimePubMsg*time.Second)
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

func (rmq *Rmq) declareReplayQueue(replayQueue replayQueueData) {
	var err error
	args := make(map[string]interface{})

	rmq.replyQueue, err = rmq.CreateQueue(CreateQueueConfig{
		Name:         replayQueue.name,
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

	var name string

	if replayQueue.exchangeType == ExchangeType.Direct {
		name = Exchanges.RmqDirect
	}

	if replayQueue.exchangeType == ExchangeType.Topic {
		name = Exchanges.RmqTopic
	}

	err = rmq.CreateExchange(CreateExchangeConfig{
		Name:       name,
		Type:       replayQueue.exchangeType,
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Args:       &args,
	})

	if err != nil {
		log.Fatal(err)
	}

	bindQueueByExchange := BindQueueByExgConfig{
		QueueName:    replayQueue.name,
		RoutingKey:   "",
		ExchangeName: name,
		NoWait:       false,
		Args:         &args,
	}

	if replayQueue.exchangeType == ExchangeType.Topic {
		bindQueueByExchange.RoutingKey = rmq.replyQueueData.rk
	}

	if replayQueue.exchangeType == ExchangeType.Direct {
		bindQueueByExchange.RoutingKey = rmq.replyQueue.Name
	}

	err = rmq.BindQueueByExchange(bindQueueByExchange)

	if err != nil {
		log.Fatal(err)
	}
}

func checkConnection(rmq *Rmq) {
	if !rmq.isConnected {
		log.Fatal("Rmqgo is not connected to rabbitmq")
	}
}

// Optionals
func WithRpc(replayQueueName, exchangeType string) RmqOption {
	if replayQueueName == "" {
		log.Fatal("Replay queue name required")
	}

	return func(rmq *Rmq) {
		validateExchangeType(exchangeType)

		if rmq.replyQueueData == nil {

			rmq.replyQueueData = &replayQueueData{
				name:         replayQueueName,
				exchangeType: exchangeType,
				rk:           "",
			}
		}

		rmq.isInitializedRpc = true
	}
}

func WithTopicRpc(replayQueueName, exchangeType, rk string) RmqOption {
	if replayQueueName == "" {
		log.Fatal("Replay queue name required")
	}

	return func(rmq *Rmq) {
		validateExchangeType(exchangeType)

		if rmq.replyQueueData == nil {

			rmq.replyQueueData = &replayQueueData{
				name:         replayQueueName,
				exchangeType: exchangeType,
				rk:           rk,
			}
		}

		rmq.isInitializedRpc = true
	}
}

func fillCreateQueueConfig(cf CreateQueueConfig) CreateQueueConfig {
	defaultMsgTtl := 30_000

	if cf.MsgTtl == nil || *cf.MsgTtl < 0 {
		ttl := defaultMsgTtl
		cf.MsgTtl = &ttl
	}

	if cf.Name == "" {
		log.Fatal("Queue name required")
	}

	return cf
}

func fillConnectionConfig(cf ConnectConfig) ConnectConfig {
	if cf.Port != "" {
		cf.Port = ":" + cf.Port + "/"
	}

	return cf
}

func validateExchangeType(exType string) {
	isValidType := exType == ExchangeType.Direct || exType == ExchangeType.Topic || ExchangeType.Fanout == exType

	if isValidType {
		return
	}

	log.Fatal("Invalid exchange type")
}
