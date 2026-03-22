.PHONY: proto build run test clean lint

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

lint:
	buf lint
	golangci-lint run ./...
