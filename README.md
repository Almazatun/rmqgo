# 🐰 rmqgo

Wrapper of [rabbitmq/amqp091-go](https://github.com/rabbitmq/amqp091-go) that provides some features.

## Installation

Inside a Go module:

```bash
go get github.com/Almazatun/rmqgo
```

## Connect rabbitMQ

```go
import (rmqgo "github.com/Almazatun/rmqgo")

    config := rmqgo.ConnectConfig{
		User: "user",
		Pass: "pass",
		Host: "host",
		Port: "port",
	}

	err := rmqgo.Connect(config)
```
