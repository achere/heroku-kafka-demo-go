TARGET := bin/heroku-kafka-demo-go

build:
	go build -race -o $(TARGET)
.PHONY: build

lint:
	golangci-lint run ./...
.PHONY: lint

test:
	GIN_MODE=release go test -race -v ./...
.PHONY: test

dev:
	PORT=4020 \
	DATABASE_URL=postgres://root:secret@localhost:5432/wms?sslmode=disable \
	KAFKA_URL=PLAINTEXT://localhost:9092 \
	KAFKA_ENV=dev \
	KAFKA_PREFIX=wms_ \
	KAFKA_TRUSTED_CERT=placeholder \
	KAFKA_CLIENT_CERT=placeholder \
	KAFKA_CLIENT_CERT_KEY=placeholder \
	go run .
