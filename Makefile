install:
	@echo 'Install rmqgo dependencies'
	go get ./...

build:
	@echo 'Build rmqgo'
	go build ./...

test:
	@echo 'Testing rmqgo tests'
	go test -v -timeout 10s -coverprofile=cover.out -cover ./...
	go tool cover -func=cover.out

test_init_rmqgo:
	@echo 'Testing init rmqgo'
	go test -v rmqgo_test.go rmqgo.go

test_producer:
	@echo 'Testing rmqgo producer'
	go test -v producer_test.go producer.go rmqgo.go consumer.go

test_consumer:
	@echo 'Testing rmqgo consumer'
	go test -v consumer_test.go consumer.go producer.go rmqgo.go
