.PHONY: proto build run test clean lint mocks

PROTO_DIR := proto
GEN_DIR := gen

proto:
	buf generate

build:
	go build -o bin/convergeplane ./cmd/convergeplane

run: build
	./bin/convergeplane

test:
	go test ./...

clean:
	rm -rf bin/

mocks:
	mockgen -source=internal/storage/repository.go -destination=internal/storage/mocks/repository.go -package=mocks

lint:
	buf lint
	golangci-lint run ./...
