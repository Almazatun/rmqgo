package rmqgo

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/exec"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer struct {
	*Rmq
	isInitialized bool
}

type ProducerInitConfig struct {
	NameQueue    string
	ExchangeName string
}

type ProducerOption func(*Producer)

func NewProducer(rmq *Rmq, options ...ProducerOption) *Producer {
	producer := &Producer{
		Rmq: rmq,
	}

	for _, opt := range options {
		opt(producer)
	}

	return producer
}

func (p *Producer) Send(ex, rk string, msg interface{}, method string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out, err := json.Marshal(SendMsg{
		Msg:    msg,
		Method: method,
	})

	if err != nil {
		log.Fatal(err)
	}

	if p.Rmq.Channel == nil {
		log.Fatal("Not initialized channel in Rmq")
	}

	err = p.Rmq.Channel.PublishWithContext(ctx,
		ex,    // exchange
		rk,    // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(out),
		})

	if err != nil {
		log.Println("Failed to declare an exchange")

		return err
	}

	return nil
}

func (p *Producer) SendReplyMsg(ex, rk string, msg interface{}, method string) (res *[]byte, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out, err := json.Marshal(SendMsg{
		Msg:    msg,
		Method: method,
	})

	if err != nil {
		log.Fatal(err)
	}

	corrIdBytes, err := exec.Command("uuidgen").Output()

	if err != nil {
		log.Fatal(err)
	}

	corrId := string(corrIdBytes)

	p.Rmq.correlationIdsMap[corrId] = corrId

	if p.Rmq.Channel == nil {
		log.Fatal("Not initialized channel in Rmq")
	}

	err = p.Rmq.Channel.PublishWithContext(ctx,
		ex,    // exchange
		rk,    // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			Body:          []byte(out),
			ReplyTo:       p.Rmq.replyQueue.Name,
			CorrelationId: corrId,
		})

	if err != nil {
		fmt.Println("Failed to declare an exchange")

		return nil, err
	}

	for msg := range p.Rmq.msgChan {
		res = &msg
		break
	}

	return res, nil
}

func WithProducerInit(config ProducerInitConfig) ProducerOption {
	var err error
	m := make(map[string]interface{})

	return func(p *Producer) {
		if p.Rmq.replyQueue == nil {
			p.Rmq.replyQueue, err = p.Rmq.CreateQueue(CreateQueueConfig{
				Name:         config.NameQueue,
				DeleteUnused: false,
				Exclusive:    false,
				NoWait:       false,
				Durable:      true,
				Args:         &m,
				MsgTtl:       nil,
			})

			if err != nil {
				log.Fatal(err)
			}

			err = p.Rmq.CreateExchange(CreateExchangeConfig{
				Name:       config.ExchangeName,
				Type:       ExchangeType.Direct,
				Durable:    true,
				AutoDelete: false,
				Internal:   false,
				NoWait:     false,
				Args:       &m,
			})

			if err != nil {
				log.Fatal(err)
			}
		}

		err = p.Rmq.BindQueueByExchange(BindQueueByExgConfig{
			p.Rmq.replyQueue.Name,
			config.NameQueue,
			config.NameQueue,
			false,
			&m,
		})

		if err != nil {
			log.Fatal(err)
		}

		p.isInitialized = true
	}
}
