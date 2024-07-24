package rmqgo

import (
	"encoding/json"
	"errors"
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

type consumerOption func(*Consumer)

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

func NewConsumer(rmq *Rmq, options ...consumerOption) *Consumer {
	consumer := &Consumer{
		Rmq:         rmq,
		handleFuncs: make(map[string]func([]byte) interface{}),
	}

	for _, opt := range options {
		opt(consumer)
	}

	return consumer
}

func WithConsumerConfig(config CreateConsumerConfig) consumerOption {
	return func(c *Consumer) {
		c.config = config
	}
}

// Make able to run in other thread when init Consumer
// It can be used if need to run rmq service with http
func WithHttpConsumer() consumerOption {
	return func(c *Consumer) {
		c.wg = &sync.WaitGroup{}
	}
}

// Consumer
func WithConsumerArgs(config ConsumerArgs) consumerOption {
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
	// Make able to run not main goroutine if need to use with HTTP
	if c.wg != nil {
		c.wg.Add(1)
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

	for d := range msgs {
		// log.Printf("Received a message: %s", d.Body)

		msg := consumerMsg{}
		err := json.Unmarshal(d.Body, &msg)

		if err != nil {
			log.Println(err)
			continue
		}

		go func() {
			// Replay msg body from created handler func by topic
			// Only read process from map without write process for that no need to use mutex
			// Write only exists when initialized rmq instance
			replayBody := c.handleTopicFunc(msg.Method, d.Body)

			// Check correlation id from other service
			_, ok := c.Rmq.correlationIdsMap[d.CorrelationId]

			if c.isReplayMsg(msg.Method, replayBody, ok) {
				c.Rmq.replay(replayMsg{
					Msg:           replayBody,
					Method:        msg.Method,
					CorrelationId: d.CorrelationId,
					ReplayTo:      d.ReplyTo,
					Exchange:      d.Exchange,
				})
			} else {
				if ok {
					// Moved delete correlation id to SendReplyMsg func
					c.Rmq.replayMsgChan <- processReplayMsg{Body: d.Body, CorrelationId: d.CorrelationId}
				} else {
					c.Rmq.msgChan <- d.Body
				}
			}
		}()

		// https://www.rabbitmq.com/confirms.html
		d.Ack(true)
	}

	if c.wg == nil {
		runnerChan := make(chan bool)

		log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
		<-runnerChan
	}
}

func (c *Consumer) AddTopicsFuncs(topicsFuncs map[string]func([]byte) interface{}) error {
	for method, f := range topicsFuncs {
		_, ok := c.handleFuncs[method]

		if ok {
			errMsg := "Already exists topic handler by " + method
			return errors.New(errMsg)
		}

		c.handleFuncs[method] = f
	}

	return nil
}

func (c *Consumer) isReplayMsg(method string, replayBody interface{}, isCorrelationIdInMap bool) bool {
	if method != "" && replayBody != nil && !isCorrelationIdInMap {
		return true
	}

	return false
}

func (c *Consumer) handleTopicFunc(method string, body []byte) (res interface{}) {
	if method != "" {
		_, ok := c.handleFuncs[method]

		if ok {
			res = c.handleFuncs[method](body)
		}
	}

	return res
}
