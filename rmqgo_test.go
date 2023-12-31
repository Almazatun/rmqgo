package rmqgo

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/joho/godotenv"
	"github.com/rabbitmq/amqp091-go"
)

var mq Rmq
var mq_Rmq_Service = *New()
var user, pass, host, port string
var q *amqp091.Queue
var producer *Producer
var consumer *Consumer

func loadENVs() {
	err := godotenv.Load(".env")

	if err != nil {
		fmt.Println("Error loading .env variables")
	}
}

func TestInitRmq(t *testing.T) {
	mq = *New(WithRpc("replay", ExchangeType.Direct))

	if reflect.ValueOf(mq).IsZero() {
		t.Fatalf("Rmq not initialized")
	}
}

func TestConnection(t *testing.T) {
	loadENVs()
	user = os.Getenv("RABBITMQ_USER")
	pass = os.Getenv("RABBITMQ_PASS")
	host = os.Getenv("RABBITMQ_HOST")
	port = os.Getenv("RABBITMQ_PORT")

	config := ConnectConfig{
		User: user,
		Pass: pass,
		Host: host,
		Port: port,
	}

	err := mq.Connect(config)

	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestCreateChannel(t *testing.T) {
	ch, err := mq.CreateChannel()

	if err != nil {
		t.Fatalf("Failed to create channel")
	}

	mq.channel = ch
}

func TestCreateQueue(t *testing.T) {
	args := make(map[string]interface{})

	queue, err := mq.CreateQueue(CreateQueueConfig{
		Name:         "test",
		DeleteUnused: false,
		Exclusive:    false,
		NoWait:       false,
		Durable:      true,
		Args:         &args,
	})

	if err != nil {
		t.Fatalf("Failed to create queue")
	}

	q = queue
}

func TestCreateExchange(t *testing.T) {
	args := make(map[string]interface{})

	err := mq.CreateExchange(CreateExchangeConfig{
		Name:       Exchanges.RmqDirect,
		Type:       ExchangeType.Direct,
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Args:       &args,
	})

	if err != nil {
		t.Fatalf("Failed to create exchange")
	}
}

func TestBindExchangeByQueue(t *testing.T) {
	args := make(map[string]interface{})

	err := mq.BindQueueByExchange(BindQueueByExgConfig{
		QueueName:    q.Name,
		RoutingKey:   q.Name,
		ExchangeName: Exchanges.RmqDirect,
		NoWait:       false,
		Args:         &args,
	})

	if err != nil {
		t.Fatalf("Failed to bind exchange by queue")
	}
}

func TestCreateProducer(t *testing.T) {
	producer = NewProducer(&mq)

	if reflect.ValueOf(producer).IsZero() {
		t.Fatalf("Rmq producer not initialized")
	}
}

func TestCreateConsumer(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	consumer := NewConsumer(
		&mq,
		WithConsumerConfig(CreateConsumerConfig{
			NameQueue: "replay",
			Consumer:  "",
			AutoAck:   false,
			Exclusive: false,
			NoWait:    false,
			NoLocal:   false,
		}),
		WithConsumerWaitGroup(wg),
	)

	if reflect.ValueOf(producer).IsZero() {
		t.Fatalf("Rmq consumer not initialized")
	}

	go consumer.Listen()
}

func TestSendMsgProducer(t *testing.T) {
	msg := "test"
	err := producer.Send(Exchanges.RmqDirect, "replay", msg, "")

	if err != nil {
		t.Fatalf("Failed to publish message")
	}

	b := <-mq.msgChan

	receivedMsg := SendMsg{}

	err = json.Unmarshal(b, &receivedMsg)

	if err != nil {
		t.Fatalf(err.Error())
	}

	if receivedMsg.Msg != msg {
		t.Fatalf("Not published message in queue")
	}
}
func TestSendMsgByMethodProducer(t *testing.T) {
	msg := "msg"
	method := "method"
	err := producer.Send(Exchanges.RmqDirect, "replay", msg, method)

	if err != nil {
		t.Fatalf("Failed to publish message")
	}

	b := <-mq.msgChan

	receivedMsg := SendMsg{}

	err = json.Unmarshal(b, &receivedMsg)

	if err != nil {
		t.Fatalf(err.Error())
	}

	if receivedMsg.Msg != msg {
		t.Fatalf("Not published message in queue")
	}
}

func TestSendReplyMsg(t *testing.T) {
	msg := "msg"
	b, err := producer.SendReplyMsg(Exchanges.RmqDirect, "replay", msg, "")

	if err != nil {
		t.Fatalf("Failed to publish message")
	}

	receivedMsg := SendMsg{}

	err = json.Unmarshal(*b, &receivedMsg)

	if err != nil {
		t.Fatalf(err.Error())
	}

	if receivedMsg.Msg != msg {
		t.Fatalf("Not published message in queue")
	}
}

func TestSendReplyMsgToService(t *testing.T) {
	s := "service"
	var consumer_service *Consumer

	config := ConnectConfig{
		User: user,
		Pass: pass,
		Host: host,
		Port: port,
	}

	err := mq_Rmq_Service.Connect(config)

	if err != nil {
		t.Fatalf(err.Error())
	}

	args := make(map[string]interface{})

	mq_Rmq_Service.replyQueue, err = mq_Rmq_Service.CreateQueue(CreateQueueConfig{
		Name:         s,
		DeleteUnused: false,
		Exclusive:    false,
		NoWait:       false,
		Durable:      true,
		Args:         &args,
	})

	mq_Rmq_Service.BindQueueByExchange(BindQueueByExgConfig{
		QueueName:    s,
		RoutingKey:   s,
		ExchangeName: Exchanges.RmqDirect,
		NoWait:       false,
		Args:         &args,
	})

	if err != nil {
		t.Fatalf("Failed to create queue")
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	consumer_service = NewConsumer(
		&mq_Rmq_Service,
		WithConsumerConfig(CreateConsumerConfig{
			NameQueue: mq_Rmq_Service.replyQueue.Name,
			Consumer:  "",
			AutoAck:   false,
			Exclusive: false,
			NoWait:    false,
			NoLocal:   false,
		}),
		WithConsumerWaitGroup(wg),
	)

	nameFunc := "createFoo"
	sendMsg := "msg"
	topicsFuncs := make(map[string]func([]byte) interface{})
	topicsFuncs[nameFunc] = createFoo

	consumer_service.AddTopicsFuncs(topicsFuncs)
	go consumer_service.Listen()

	b, err := producer.SendReplyMsg(Exchanges.RmqDirect, s, sendMsg, nameFunc)

	if err != nil {
		t.Fatalf("Failed to publish message")
	}

	receivedMsg := SendMsg{}

	err = json.Unmarshal(*b, &receivedMsg)

	if err != nil {
		t.Fatalf(err.Error())
	}

	if receivedMsg.Msg != createFoo([]byte{}) {
		t.Fatalf("Not published message in queue")
	}
}

func TestSendMsgByTopic(t *testing.T) {
	mq_Rmq_Topic := New(WithTopicRpc("logs_topic", ExchangeType.Topic, "#"))

	config := ConnectConfig{
		User: user,
		Pass: pass,
		Host: host,
		Port: port,
	}

	mq_Rmq_Topic.Connect(config)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	consumer := NewConsumer(
		mq_Rmq_Topic,
		WithConsumerConfig(CreateConsumerConfig{
			NameQueue: "logs_topic",
			Consumer:  "",
			AutoAck:   false,
			Exclusive: false,
			NoWait:    false,
			NoLocal:   false,
		}),
		WithConsumerWaitGroup(wg),
	)

	go consumer.Listen()

	p := NewProducer(mq_Rmq_Topic)

	msg := "log"
	err := p.Send(Exchanges.RmqTopic, "logs_topic", msg, "")

	if err != nil {
		t.Fatalf("Failed to publish message")
	}

	b := <-mq_Rmq_Topic.msgChan

	receivedMsg := SendMsg{}

	err = json.Unmarshal(b, &receivedMsg)

	if err != nil {
		t.Fatalf(err.Error())
	}

	if receivedMsg.Msg != msg {
		t.Fatalf("Not published message in queue")
	}

	err = mq_Rmq_Topic.Close()

	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestCloseRmq(t *testing.T) {
	err := mq.Close()

	if err != nil {
		t.Fatalf(err.Error())
	}

	err = mq_Rmq_Service.Close()

	if err != nil {
		t.Fatalf(err.Error())
	}
}

func createFoo(b []byte) interface{} {
	return "CreatedFoo"
}
