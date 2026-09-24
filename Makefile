SHELL := /bin/bash
SERVICES := gateway accounts transfers notifications reconciler
GOBIN ?= $(shell go env GOPATH)/bin

.PHONY: help
help: ## Показать команды
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: tools
tools: ## Установить buf, protoc-gen-go(-grpc), goose, golangci-lint (версия как в CI)
	go install github.com/bufbuild/buf/cmd/buf@v1.57.2
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install github.com/pressly/goose/v3/cmd/goose@latest
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.2

.PHONY: proto
proto: ## Сгенерировать gRPC-код из api/proto в gen/
	PATH=$(GOBIN):$$PATH buf lint
	PATH=$(GOBIN):$$PATH buf generate

.PHONY: build
build: ## Собрать все сервисы в bin/
	@for s in $(SERVICES); do CGO_ENABLED=0 go build -o bin/$$s ./cmd/$$s || exit 1; done

.PHONY: lint
lint: ## golangci-lint
	golangci-lint run ./...

.PHONY: test
test: ## Unit-тесты с race detector
	go test -race -count=1 ./internal/...

.PHONY: test-integration
test-integration: ## Интеграционные тесты (нужен Docker: testcontainers)
	go test -race -count=1 -tags=integration -timeout=10m ./tests/integration/...

.PHONY: cover
cover: ## Покрытие unit-тестами
	go test -coverprofile=coverage.out ./internal/... && go tool cover -func=coverage.out | tail -1

.PHONY: up
up: ## Поднять всё окружение
	docker compose up -d --build

.PHONY: infra
infra: ## Поднять только инфраструктуру (для go run локально)
	docker compose up -d postgres redis redpanda redpanda-console jaeger otel-collector prometheus grafana

.PHONY: down
down: ## Остановить окружение
	docker compose down

.PHONY: clean
clean: ## Остановить окружение и удалить данные
	docker compose down -v

.PHONY: logs
logs: ## Логи сервисов
	docker compose logs -f gateway accounts transfers notifications

.PHONY: reconcile
reconcile: ## Запустить сверку
	docker compose run --rm reconciler

.PHONY: load
load: ## Нагрузочный тест k6
	k6 run tests/load/transfers.js

.PHONY: migration
migration: ## Новая миграция: make migration svc=accounts name=add_x
	goose -dir migrations/$(svc) create $(name) sql
