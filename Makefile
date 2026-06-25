.PHONY: proto proto-python build run test clean lint mocks db-start db-stop db-create db-migrate db-reset

PROTO_DIR := proto
GEN_DIR := gen

# Regenerate stubs for all languages. buf covers Go and C#; Python uses
# grpcio-tools because buf's remote python plugin targets a newer protobuf
# runtime (7.x gencode) than the SDK's grpcio/protobuf 6.x stack.
proto:
	buf generate
	$(MAKE) proto-python

# Python-only regeneration. Use a single grpcio-tools version (the sdk dev extra
# pins grpcio-tools>=1.81.0) so the generated stubs' version stamps stay aligned.
proto-python:
	python -m grpc_tools.protoc -I proto --python_out=sdk/python --grpc_python_out=sdk/python proto/convergeplane/v1/*.proto

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
