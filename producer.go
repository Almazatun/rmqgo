package rmqgo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os/exec"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer struct {
	rmq *Rmq
}

type ProducerInitConfig struct {
	NameQueue    string
	ExchangeName string
}

type producerOption func(*Producer)

func NewProducer(rmq *Rmq, options ...producerOption) *Producer {
	producer := &Producer{
		rmq: rmq,
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

	if p.rmq.channel == nil {
		log.Fatal("Not initialized channel in Rmq")
	}

	err = p.rmq.channel.PublishWithContext(ctx,
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

func (p *Producer) SendReply(ex, rk string, msg interface{}, method string) (res []byte, err error) {
	if !p.rmq.isInitializedRpc {
		return nil, errors.New("initialize rmq with RPC mode to use SendReplayMsg")
	}

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

	p.rmq.mu.Lock()
	p.rmq.correlationIdsMap[corrId] = corrId
	p.rmq.mu.Unlock()

	if p.rmq.channel == nil {
		log.Fatal("Not initialized channel in Rmq")
	}

	err = p.rmq.channel.PublishWithContext(ctx,
		ex,    // exchange
		rk,    // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			Body:          []byte(out),
			ReplyTo:       p.rmq.replyQueue.Name,
			CorrelationId: corrId,
		})

	if err != nil {
		fmt.Println("Failed to declare an exchange")

		return nil, err
	}

	res = p.listenReplayMsg(ctx, corrId)

	if res == nil {
		return nil, errors.New("Response timeout by sending replay message")
	}

	return res, nil
}

func (p *Producer) listenReplayMsg(ctx context.Context, correlationId string) []byte {
	replayMsgBytes, ok := p.rmq.replayMsgMap[correlationId]

	if ok {
		delete(p.rmq.replayMsgMap, correlationId)
		return replayMsgBytes
	}

	for {
		select {
		case <-ctx.Done():
			delete(p.rmq.replayMsgMap, correlationId)
			return nil
		case msg := <-p.rmq.replayMsgChan:
			if msg.CorrelationId != correlationId {
				p.rmq.mu.Lock()
				p.rmq.replayMsgMap[msg.CorrelationId] = msg.Body
				p.rmq.mu.Unlock()
			} else {
				delete(p.rmq.replayMsgMap, correlationId)
				return msg.Body
			}
		}
	}
}
