# 🐰 rmqgo

[![Go Version](https://img.shields.io/badge/go-1.22+-00ADD8?style=flat-square&logo=go)](https://golang.org/)
[![License](https://img.shields.io/badge/license-MIT-0969da?style=flat-square&logo=opensource)](https://opensource.org/licenses/MIT)

Wrapper of [rabbitmq/amqp091-go](https://github.com/rabbitmq/amqp091-go) that provides some features.

## Installation

```bash
go get github.com/Almazatun/rmqgo
```

## Connect to rabbitMQ

```go
import (rmqgo "github.com/Almazatun/rmqgo")

rmq := rmqgo.New()

config := rmqgo.ConnectConfig{
 User: "user",
 Pass: "pass",
 Host: "host",
 Port: "port",
}

err := rmq.Connect(config)

if err != nil {
 // some action
}
```

### Optional params when initialize `rmqgo.New()`

With RPC mode for `request and replay pattern`

```go
rmqgo.New(rmqgo.WithRpc(replayQueueName, exchangeType))
```

With topic RPC

```go
rmqgo.New(rmqgo.WithTopicRpc(replayQueueName, exchangeType, routingKey))
```

### Create channel

```go
ch, err := rmq.CreateChannel()

if err != nil {
 // some action
}
```

### Create queue

```go
args := make(map[string]interface{})

q, err := rmq.CreateQueue(rmqgo.CreateQueueConfig{
 Name:         "some_name",
 DeleteUnused: false,
 Exclusive:    false,
 NoWait:       false,
 Durable:      true,
 Args:         &args,
})

if err != nil {
 // some action
}

```

### Create exchange

Exchanges

```go
import (rmqgo "github.com/Almazatun/rmqgo")

rmqgo.Exchanges.Direct()
rmqgo.Exchanges.Topic()
rmqgo.Exchanges.Fanout()
rmqgo.Exchanges.Headers()
```

Exchange types

```go
import (rmqgo "github.com/Almazatun/rmqgo")

rmqgo.ExchangeType.Direct()
rmqgo.ExchangeType.Topic()
rmqgo.ExchangeType.Fanout()
```

```go
args := make(map[string]interface{})

err := rmq.CreateExchange(rmqgo.CreateExchangeConfig{
 Name:       rmqgo.Exchanges.RmqDirect,
 Type:       rmqgo.ExchangeType.Direct,
 Durable:    true,
 AutoDelete: false,
 Internal:   false,
 NoWait:     false,
 Args:       &args,
})

if err != nil {
 // some action
}
```

### Bind exchange by created queue

```go
args := make(map[string]interface{})

err := rmq.BindQueueByExchange(rmqgo.BindQueueByExgConfig{
 QueueName:    "some_name",
 RoutingKey:   "some_key",
 ExchangeName: Exchanges.RmqDirect,
 NoWait:       false,
 Args:         &args,
})

if err != nil {
 // some action
}
```

## Create `producer`

```go
producer = rmqgo.NewProducer(&rmq)
```

### Send message

```go
err := producer.Send(Exchanges.RmqDirect, routingKey, msg, method)

if err != nil {
 // some action
}

```

### Send message with reply

```go
b, err := producer.SendReply(Exchanges.RmqDirect, routingKey, msg, method)

if err != nil {
 // some action
}

// msg - is your own type SomeName struct { someFields:... }

err = json.Unmarshal(*b, &msg)

if err != nil {
 // some action
}

```

## Create `consumer`

```go
consumer := rmqgo.NewConsumer(
  &rmq,
  rmqgo.WithConsumerConfig(rmqgo.CreateConsumerConfig{
   NameQueue: "some_name",
   Consumer:  "some_value",
   AutoAck:   false,
   Exclusive: false,
   NoWait:    false,
   NoLocal:   false,
  }),
 )

consumer.Listen()
```

Consuming messages from queues

```go
// Bytes - <- chan []byte
<- rmq.ReceiveMessages()
```

```go
consumer.Listen()
```

### Optional params when initialize `rmqgo.NewConsumer(...)`

With `HttpConsumer`

```go
rmqgo.NewConsumer(*rmq, rmqgo.WithHttpConsumer())
```

With `Consumer Args`

```go
rmqgo.NewConsumer(*rmq, rmqgo.WithConsumerArgs(rmqgo.ConsumerArgs{
 XDeadLetterExc        *""
 XDeadLetterRoutingKey *""
 Ttl                   *int
 XExpires              *int
 XMaxPriority          *int
}))
```
