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
	*Rmq
}

type ProducerInitConfig struct {
	NameQueue    string
	ExchangeName string
}

type producerOption func(*Producer)

func NewProducer(rmq *Rmq, options ...producerOption) *Producer {
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

	if p.Rmq.channel == nil {
		log.Fatal("Not initialized channel in Rmq")
	}

	err = p.Rmq.channel.PublishWithContext(ctx,
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

func (p *Producer) SendReplyMsg(ex, rk string, msg interface{}, method string) (res []byte, err error) {
	if !p.Rmq.isInitializedRpc {
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

	p.Rmq.mu.Lock()
	p.Rmq.correlationIdsMap[corrId] = corrId
	p.Rmq.mu.Unlock()

	if p.Rmq.channel == nil {
		log.Fatal("Not initialized channel in Rmq")
	}

	err = p.Rmq.channel.PublishWithContext(ctx,
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

	res = p.listenReplayMsg(ctx, corrId)

	return res, nil
}

func (p *Producer) listenReplayMsg(ctx context.Context, correlationId string) []byte {
	replayMsgBytes, ok := p.Rmq.replayMsgMap[correlationId]

	if ok {
		delete(p.Rmq.replayMsgMap, correlationId)
		return replayMsgBytes
	}

	for {
		select {
		case <-ctx.Done():
			delete(p.Rmq.replayMsgMap, correlationId)
			fmt.Println("Response timeout by sending replay message")
			return nil
		case msg := <-p.Rmq.replayMsgChan:
			if msg.CorrelationId != correlationId {
				p.Rmq.mu.Lock()
				p.Rmq.replayMsgMap[msg.CorrelationId] = msg.Body
				p.Rmq.mu.Unlock()
			} else {
				delete(p.Rmq.replayMsgMap, correlationId)
				return msg.Body
			}
		}
	}
}
