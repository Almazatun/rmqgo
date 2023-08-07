package rmqgo

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/joho/godotenv"
	"github.com/rabbitmq/amqp091-go"
)

var mq Rmq = *New(WithRpc("replay"))
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

	consumer.Listen()
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

	consumer_service.AddHandleFunc(nameFunc, createFoo)
	consumer_service.Listen()

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

func createFoo(b []byte) interface{} {
	return "CreatedFoo"
}
