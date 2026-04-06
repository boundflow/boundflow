.PHONY: proto build run test clean lint mocks db-start db-stop db-create db-migrate db-reset

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

DB_URL ?= postgres://localhost:5432/convergeplane?sslmode=disable
MIGRATIONS_DIR := migrations

db-start:
	pg_ctl -D /usr/local/var/postgres start

db-stop:
	pg_ctl -D /usr/local/var/postgres stop

db-create:
	psql postgres -c "CREATE DATABASE convergeplane;"

db-migrate:
	migrate -path $(MIGRATIONS_DIR) -database "$(DB_URL)" up

db-reset:
	migrate -path $(MIGRATIONS_DIR) -database "$(DB_URL)" drop -f
	migrate -path $(MIGRATIONS_DIR) -database "$(DB_URL)" up
