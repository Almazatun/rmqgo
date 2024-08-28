package main

import (
	"encoding/json"
	"fmt"
	"os"

	rmqgo "github.com/Almazatun/rmqgo"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load("./.env_example")

	if err != nil {
		panic(err)
	}

	rmq := rmqgo.New(rmqgo.WithRpc("replay_test", rmqgo.ExchangeType.Direct()))
	err = rmq.Connect(rmqgo.ConnectConfig{
		Host: os.Getenv("RMQGO_HOST"),
		Port: os.Getenv("RMQGO_PORT"),
		User: os.Getenv("RMQGO_USER"),
		Pass: os.Getenv("RMQGO_PASS"),
	})

	if err != nil {
		panic(err)
	}

	// Producer
	producer := rmqgo.NewProducer(rmq)

	// Consumer
	args := make(map[string]interface{})
	q, err := rmq.CreateQueue(rmqgo.CreateQueueConfig{
		Name:         "queue_test",
		DeleteUnused: false,
		Exclusive:    false,
		NoWait:       false,
		Durable:      true,
		Args:         &args,
	})

	if err != nil {
		panic(err)
	}

	err = rmq.CreateExchange(rmqgo.CreateExchangeConfig{
		Name:       rmqgo.Exchanges.Direct(),
		Type:       rmqgo.ExchangeType.Direct(),
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Args:       &args,
	})

	if err != nil {
		panic(err)
	}

	err = rmq.BindQueueByExchange(rmqgo.BindQueueByExgConfig{
		RoutingKey:   q.Name,
		QueueName:    q.Name,
		ExchangeName: rmqgo.Exchanges.Direct(),
		NoWait:       false,
		Args:         &args,
	})

	if err != nil {
		panic(err)
	}

	consumer := rmqgo.NewConsumer(
		rmq,
		rmqgo.WithConsumerConfig(rmqgo.CreateConsumerConfig{
			NameQueue: q.Name,
			Consumer:  "",
			AutoAck:   false,
			Exclusive: false,
			NoWait:    false,
			NoLocal:   false,
		}),
		rmqgo.WithHttpConsumer(),
	)

	go consumer.Listen()

	b, err := producer.SendReplyMsg(rmqgo.Exchanges.RmqDirect, q.Name, "Hello RMQGO 🐰", "")

	if err != nil {
		panic(err)
	}

	receivedMsg := rmqgo.SendMsg{}
	err = json.Unmarshal(b, &receivedMsg)

	if err != nil {
		panic(err)
	}

	fmt.Println(receivedMsg.Msg)
	// "Hello RMQGO 🐰"
}
