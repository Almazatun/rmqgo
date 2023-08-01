install:
	@echo 'Install dependencies'
	go get ./...

build:
	@echo 'Build lib'
	go build ./...

test:
	@echo 'Testing lib'
	go test -v -timeout 30s -coverprofile=cover.out -cover ./...
	go tool cover -func=cover.out
