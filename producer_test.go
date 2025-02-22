package rmqgo

import (
	"encoding/json"
	"reflect"
	"sync"
	"testing"

	"github.com/Almazatun/rmqgo/util"
)

var rmqgoProducer Rmq
var rmqgoOtherService = *New()
var producer *Producer
var testQueueName = "replay"
var testConsumer *Consumer

func initRmqgo(t *testing.T) {
	rmqgoProducer = *New(WithRpc(testQueueName, ExchangeType.Direct()))

	if reflect.ValueOf(rmqgoProducer).IsZero() {
		t.Fatalf("Rmq not initialized")
	}
}

func TestCreateProducer(t *testing.T) {
	envs := util.GetENVs()

	if envs == nil {
		t.Fatalf("Rmqgo envs not set")
	}

	config := ConnectConfig{User: envs.User, Pass: envs.Pass, Host: envs.Host, Port: envs.Port}

	initRmqgo(t)

	err := rmqgoProducer.Connect(config)

	if err != nil {
		t.Fatalf(err.Error())
	}

	producer = NewProducer(&rmqgoProducer)

	if reflect.ValueOf(producer).IsZero() {
		t.Fatalf("Rmq producer not initialized")
	}

	createConsumerListener(t)
}

func createConsumerListener(t *testing.T) {

	testConsumer = NewConsumer(
		&rmqgoProducer,
		WithConsumerConfig(CreateConsumerConfig{
			NameQueue: testQueueName,
			Consumer:  "",
			AutoAck:   false,
			Exclusive: false,
			NoWait:    false,
			NoLocal:   false,
		}),
		WithHttpConsumer(),
	)

	if reflect.ValueOf(producer).IsZero() {
		t.Fatalf("Rmq consumer not initialized")
	}

	go testConsumer.Listen()
}

func TestSendMsgProducer(t *testing.T) {
	msg := "test"
	err := producer.Send(Exchanges.Direct(), testQueueName, msg, "")

	if err != nil {
		t.Fatalf("Failed to publish message")
	}

	b := <-rmqgoProducer.ReceiveMessages()

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
	err := producer.Send(Exchanges.Direct(), testQueueName, msg, method)

	if err != nil {
		t.Fatalf("Failed to publish message")
	}

	b := <-rmqgoProducer.ReceiveMessages()

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
	sendMsgList := []string{"msg", "msg2"}

	for _, msg := range sendMsgList {
		b, err := producer.SendReply(Exchanges.Direct(), testQueueName, msg, "")

		if err != nil {
			t.Fatalf("Failed to publish message: %v", msg)
		}

		receivedMsg := SendMsg{}

		err = json.Unmarshal(b, &receivedMsg)

		if err != nil {
			t.Fatalf(err.Error())
		}

		if receivedMsg.Msg != msg {
			t.Fatalf("Not published message in queue")
		}
	}
}

func TestSendReplyMsgConcurrent(t *testing.T) {
	sendMsgList := []string{"msg", "msg2", "msg3", "msg4", "msg5"}
	wg := sync.WaitGroup{}

	for _, msg := range sendMsgList {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, err := producer.SendReply(Exchanges.Direct(), testQueueName, msg, "")

			if err != nil {
				t.Fatalf("Failed to publish message: %v", msg)
			}

			receivedMsg := SendMsg{}

			err = json.Unmarshal(b, &receivedMsg)

			if err != nil {
				t.Fatalf(err.Error())
			}

			if receivedMsg.Msg != msg {
				t.Fatalf("Not published message in queue")
			}
		}()
	}

	wg.Wait()
}

func TestSendReplyMsgByOtherService(t *testing.T) {
	s := "service"
	var consumerService *Consumer

	envs := util.GetENVs()

	if envs == nil {
		t.Fatalf("Rmqgo envs not set")
	}

	config := ConnectConfig{User: envs.User, Pass: envs.Pass, Host: envs.Host, Port: envs.Port}

	err := rmqgoOtherService.Connect(config)

	if err != nil {
		t.Fatalf(err.Error())
	}

	args := make(map[string]interface{})

	rmqgoOtherService.replyQueue, err = rmqgoOtherService.CreateQueue(CreateQueueConfig{
		Name:         s,
		DeleteUnused: false,
		Exclusive:    false,
		NoWait:       false,
		Durable:      true,
		Args:         &args,
	})

	rmqgoOtherService.BindQueueByExchange(BindQueueByExgConfig{
		QueueName:    s,
		RoutingKey:   s,
		ExchangeName: Exchanges.Direct(),
		NoWait:       false,
		Args:         &args,
	})

	if err != nil {
		t.Fatalf("Failed to create queue")
	}

	consumerService = NewConsumer(
		&rmqgoOtherService,
		WithConsumerConfig(CreateConsumerConfig{
			NameQueue: rmqgoOtherService.replyQueue.Name,
			Consumer:  "",
			AutoAck:   false,
			Exclusive: false,
			NoWait:    false,
			NoLocal:   false,
		}),
		WithHttpConsumer(),
	)

	nameFunc := "createFoo"
	sendMsg := "msg"
	topicsFuncs := make(map[string]func([]byte) interface{})
	topicsFuncs[nameFunc] = createFoo

	consumerService.AddTopicsFuncs(topicsFuncs)
	go consumerService.Listen()

	b, err := producer.SendReply(Exchanges.Direct(), s, sendMsg, nameFunc)

	if err != nil {
		t.Fatalf("Failed to publish message")
	}

	receivedMsg := SendMsg{}

	err = json.Unmarshal(b, &receivedMsg)

	if err != nil {
		t.Fatalf(err.Error())
	}

	if receivedMsg.Msg != createFoo([]byte{}) {
		t.Fatalf("Not published message in queue")
	}
}

func TestSendMsgByTopic(t *testing.T) {
	rmqgoTopic := New(WithTopicRpc("logs_topic", ExchangeType.Topic(), "#"))

	envs := util.GetENVs()
	config := ConnectConfig{User: envs.User, Pass: envs.Pass, Host: envs.Host, Port: envs.Port}

	if envs == nil {
		t.Fatalf("Rmqgo envs not set")
	}

	rmqgoTopic.Connect(config)

	testConsumer := NewConsumer(
		rmqgoTopic,
		WithConsumerConfig(CreateConsumerConfig{
			NameQueue: "logs_topic",
			Consumer:  "",
			AutoAck:   false,
			Exclusive: false,
			NoWait:    false,
			NoLocal:   false,
		}),
		WithHttpConsumer(),
	)

	go testConsumer.Listen()

	p := NewProducer(rmqgoTopic)

	msg := "log"
	err := p.Send(Exchanges.Topic(), "logs_topic", msg, "")

	if err != nil {
		t.Fatalf("Failed to publish message")
	}

	b := <-rmqgoTopic.ReceiveMessages()

	receivedMsg := SendMsg{}

	err = json.Unmarshal(b, &receivedMsg)

	if err != nil {
		t.Fatalf(err.Error())
	}

	if receivedMsg.Msg != msg {
		t.Fatalf("Not published message in queue")
	}

	err = rmqgoTopic.Close()

	if err != nil {
		t.Fatalf(err.Error())
	}

	err = rmqgoProducer.Close()

	if err != nil {
		t.Fatalf(err.Error())
	}

	err = rmqgoOtherService.Close()

	if err != nil {
		t.Fatalf(err.Error())
	}
}

func createFoo(b []byte) interface{} {
	return "CreatedFoo"
}
