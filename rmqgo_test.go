package rmqgo

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/joho/godotenv"
)

var mq Rmq = *New()
var user, pass, host, port string

func loadENVs() {
	err := godotenv.Load(".env")

	if err != nil {
		fmt.Println("Error loading .env variables")
	}
}

func TestConnection(t *testing.T) {
	loadENVs()
	user = os.Getenv("RABBITMQ_USER")
	pass = os.Getenv("RABBITMQ_PASS")
	host = os.Getenv("RABBITMQ_HOST")
	port = os.Getenv("RABBITMQ_PORT")

	config := ConnectConfig{
		User:         user,
		Pass:         pass,
		Host:         host,
		Port:         port,
		IsInit:       false,
		NameQueue:    nil,
		ExchangeName: nil,
	}

	mq.Connect(config)

	if mq.isConnected == false {
		t.Fatalf("Connection failed")
	}
}

func TestConnectionWithInitMode(t *testing.T) {
	test := "test"

	config := ConnectConfig{
		User:         user,
		Pass:         pass,
		Host:         host,
		Port:         port,
		IsInit:       true,
		NameQueue:    &test,
		ExchangeName: &test,
	}

	mq.Connect(config)

	if !mq.isConnected {
		t.Fatalf("Connection failed")
	}

	if !mq.isInitialized {
		t.Fatalf("Connection with init flag failed")
	}
}

func TestCreateConsumer(t *testing.T) {
	if !mq.isConnected {
		t.Fatalf("Not connect to rabbit MQ")
	}

	if !mq.isInitialized {
		t.Fatalf("Not init channel")
	}

	config := CreateConsumerConfig{
		mq.replyQueue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	config.Wg = wg

	mq.CreateConsumer(config, make(map[string]func([]byte) interface{}))

	if !mq.isCreatedConsumer {
		t.Fatalf("Failed to create consumer")
	}
}

func TestSendMsg(t *testing.T) {
	if !mq.isConnected {
		t.Fatalf("Not connect to rabbit MQ")
	}

	if !mq.isInitialized {
		t.Fatalf("Not init channel")
	}
	sendMsg := "Hello"
	err := mq.Send("test", "test", sendMsg, "")

	if err != nil {
		t.Fatalf("Failed to publish message")
	}

	b := <-mq.messageChan
	receivedMsg := SendMsg{}

	err = json.Unmarshal(b, &receivedMsg)

	if err != nil {
		t.Fatalf(err.Error())
	}

	if receivedMsg.Msg != sendMsg {
		t.Fatalf("Not published message in queue")
	}

}

func TestSendMsgByMethod(t *testing.T) {
	if !mq.isConnected {
		t.Fatalf("Not connect to rabbit MQ")
	}

	if !mq.isInitialized {
		t.Fatalf("Not init channel")
	}

	m := "test_method"
	sendMsg := "TEST"
	err := mq.Send("test", "test", sendMsg, m)

	if err != nil {
		t.Fatalf("Failed to publish message")
	}

	b := <-mq.messageChan
	receivedMsg := SendMsg{}

	err = json.Unmarshal(b, &receivedMsg)

	if err != nil {
		t.Fatalf(err.Error())
	}

	if receivedMsg.Msg != sendMsg {
		t.Fatalf("Not published message in queue")
	}
}

func TestSendReplyMsg(t *testing.T) {
	if !mq.isConnected {
		t.Fatalf("Not connect to rabbit MQ")
	}

	if !mq.isInitialized {
		t.Fatalf("Not init channel")
	}

	m := "test_method"
	sendMsg := "Replay"
	b, err := mq.SendReplyMsg("test", "test", sendMsg, m)

	if err != nil {
		t.Fatalf("Failed to publish message")
	}

	receivedMsg := SendMsg{}

	err = json.Unmarshal(*b, &receivedMsg)

	if err != nil {
		t.Fatalf(err.Error())
	}

	if receivedMsg.Msg != sendMsg {
		t.Fatalf("Not published message in queue")
	}
}
