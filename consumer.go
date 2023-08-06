package rmqgo

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	*Rmq
	handleFuncs map[string]func([]byte) interface{}
	wg          *sync.WaitGroup
	args        amqp.Table
	config      CreateConsumerConfig
	msg         chan amqp.Delivery
}

type ConsumerOption func(*Consumer)

type ConsumerArgs struct {
	XDeadLetterExc        *string
	XDeadLetterRoutingKey *string
	Ttl                   *int
	XExpires              *int
	XMaxPriority          *int
}

type CreateConsumerConfig struct {
	NameQueue string
	Consumer  string
	AutoAck   bool
	Exclusive bool
	NoWait    bool
	NoLocal   bool
}

type consumerMsg struct {
	Method string
	Msg    interface{}
}

func NewConsumer(rmq *Rmq, options ...ConsumerOption) *Consumer {
	consumer := &Consumer{
		Rmq:         rmq,
		handleFuncs: make(map[string]func([]byte) interface{}),
	}

	for _, opt := range options {
		opt(consumer)
	}

	return consumer
}

func WithConsumerConfig(config CreateConsumerConfig) ConsumerOption {
	return func(c *Consumer) {
		c.config = config
	}
}

// Make able to run in other thread when init Consumer
// It can be used if need to run rmq service with http
func WithConsumerWaitGroup(wg *sync.WaitGroup) ConsumerOption {
	return func(c *Consumer) {
		c.wg = wg
	}
}

// Consumer
func WithConsumerArgs(config ConsumerArgs) ConsumerOption {
	args := amqp.Table{}

	if config.Ttl != nil {
		args["x-message-ttl"] = int64(*config.Ttl)
	}

	if config.XDeadLetterExc != nil && *config.XDeadLetterExc != "" {
		args["x-dead-letter-exchange"] = config.XDeadLetterExc
	}

	if config.XDeadLetterRoutingKey != nil && *config.XDeadLetterRoutingKey != "" {
		args["x-dead-letter-routing-key"] = config.XDeadLetterRoutingKey
	}

	if config.XExpires != nil {
		args["x-expires"] = int64(*config.XExpires)
	}

	if config.XMaxPriority != nil {
		args["x-max-priority"] = int8(*config.XMaxPriority)
	}

	return func(c *Consumer) {
		c.args = args
	}
}

func (c *Consumer) Listen() {
	if c.wg != nil {
		defer c.wg.Done()
	}

	if c.Rmq.channel == nil {
		log.Fatal("Not initialized channel in Rmq")
	}

	msgs, err := c.Rmq.channel.Consume(
		c.config.NameQueue, // queue
		c.config.Consumer,  // consumer
		c.config.AutoAck,
		c.config.Exclusive,
		c.config.NoLocal,
		c.config.NoWait,
		c.args,
	)

	if err != nil {
		log.Fatal(err)
	}

	runnerChan := make(chan bool)

	go func() {
		for d := range msgs {
			// fmt.Printf("Received a message: %s", d.Body)

			msg := consumerMsg{}
			err := json.Unmarshal(d.Body, &msg)

			if err != nil {
				fmt.Println(err)
				continue
			}

			var resMas interface{}

			if msg.Method != "" {
				_, ok := c.handleFuncs[msg.Method]

				if ok {
					resMas = c.handleFuncs[msg.Method](d.Body)
				}
			}

			_, ok := c.Rmq.correlationIdsMap[d.CorrelationId]

			if d.CorrelationId != "" && msg.Method != "" && resMas != nil && !ok {
				c.Rmq.replay(replayMsg{
					Msg:           resMas,
					Method:        msg.Method,
					CorrelationId: d.CorrelationId,
					ReplayTo:      d.ReplyTo,
					Exchange:      d.Exchange,
				})
			} else {

				if ok {
					delete(c.Rmq.correlationIdsMap, d.CorrelationId)
				}

				c.Rmq.msgChan <- d.Body
			}
			d.Ack(true)
		}
	}()

	if c.wg == nil {
		log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
		<-runnerChan
	}
}

func (c *Consumer) AddHandleFunc(method string, f func([]byte) interface{}) {
	_, ok := c.handleFuncs[method]

	if ok {
		log.Printf("Handler func already exists in map by method %s", method)
		return
	}

	c.handleFuncs[method] = f
}
