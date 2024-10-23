package rmqgo

import (
	"encoding/json"
	"log"
	"reflect"
	"testing"

	"github.com/Almazatun/rmqgo/util"
)

var rmqgoConsumer Rmq
var consumer *Consumer
var testQueue = "test_test"

func TestCreateConsumer(t *testing.T) {
	envs := util.GetENVs()

	if envs == nil {
		t.Fatalf("Rmqgo envs not set")
	}

	config := ConnectConfig{User: envs.User, Pass: envs.Pass, Host: envs.Host, Port: envs.Port}

	rmqgoConsumer = *New()

	if reflect.ValueOf(rmqgoConsumer).IsZero() {
		t.Fatalf("Rmq not initialized")
	}

	err := rmqgoConsumer.Connect(config)
	args := make(map[string]interface{})

	rmqgoConsumer.CreateQueue(CreateQueueConfig{
		Name:         testQueue,
		DeleteUnused: false,
		Exclusive:    false,
		NoWait:       false,
		Durable:      false,
		Args:         &args,
	})

	if err != nil {
		t.Fatalf(err.Error())
	}

	rmqgoConsumer.CreateExchange(CreateExchangeConfig{
		Name:       Exchanges.Direct(),
		Type:       ExchangeType.Direct(),
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Args:       &args,
	})

	if err != nil {
		log.Fatal(err)
	}

	err = rmqgoConsumer.BindQueueByExchange(BindQueueByExgConfig{
		RoutingKey:   testQueue,
		QueueName:    testQueue,
		ExchangeName: Exchanges.Direct(),
		NoWait:       false,
		Args:         &args,
	})

	if err != nil {
		log.Fatal(err)
	}

	consumer = NewConsumer(
		&rmqgoConsumer,
		WithConsumerConfig(CreateConsumerConfig{
			NameQueue: testQueue,
			Consumer:  "",
			AutoAck:   false,
			Exclusive: false,
			NoWait:    false,
			NoLocal:   false,
		}),
		WithHttpConsumer(),
	)

	if reflect.ValueOf(consumer).IsZero() {
		t.Fatalf("Rmq consumer not initialized")
	}

	go consumer.Listen()
}

func TestConsumerListener(t *testing.T) {
	testProducer := NewProducer(&rmqgoConsumer)

	msg := "hello"
	err := testProducer.Send(Exchanges.Direct(), testQueue, msg, "")

	if err != nil {
		t.Fatalf("Failed to publish message")
	}

	b := <-rmqgoConsumer.MsgChan

	receivedMsg := SendMsg{}

	err = json.Unmarshal(b, &receivedMsg)

	if err != nil {
		t.Fatalf(err.Error())
	}

	if receivedMsg.Msg != msg {
		t.Fatalf("Not published message in queue")
	}

	err = rmqgoConsumer.Close()

	if err != nil {
		t.Fatalf(err.Error())
	}
}
