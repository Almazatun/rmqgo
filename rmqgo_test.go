package rmqgo

import (
	"reflect"
	"testing"

	util "github.com/Almazatun/rmqgo/util"
	"github.com/rabbitmq/amqp091-go"
)

var mq Rmq

var user, pass, host, port string
var q *amqp091.Queue

func TestInitRmqgo(t *testing.T) {
	mq = *New(WithRpc("replay", ExchangeType.Direct()))

	if reflect.ValueOf(mq).IsZero() {
		t.Fatalf("Rmq not initialized")
	}
}

func TestConnection(t *testing.T) {
	envs := util.GetENVs()
	config := ConnectConfig{User: envs.User, Pass: envs.Pass, Host: envs.Host, Port: envs.Port}

	if envs == nil {
		t.Fatalf("Rmqgo envs not set")
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
		Name:       Exchanges.Direct(),
		Type:       ExchangeType.Direct(),
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
		ExchangeName: Exchanges.Direct(),
		NoWait:       false,
		Args:         &args,
	})

	if err != nil {
		t.Fatalf("Failed to bind exchange by queue")
	}
}

func TestCloseRmqgo(t *testing.T) {
	err := mq.Close()

	if err != nil {
		t.Fatalf(err.Error())
	}
}
