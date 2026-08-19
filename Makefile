.PHONY: build test test-race test-integration test-llm-executor test-agent test-mysql-foundation test-mysql-lab test-mysql-concurrency report-mysql-explain smoke-phase5 smoke-llm smoke-agent smoke-agent-eval demo verify lint fmt tools proto compose-up compose-down migrate-up migrate-down run-server

GO ?= go
COMPOSE := docker compose -f deploy/docker-compose.yml

build:
	$(GO) build ./cmd/...

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

test-integration:
	$(GO) test -tags=integration -count=1 ./tests/integration/...

test-llm-executor:
	$(GO) test -race -count=1 ./internal/config ./internal/executor/llm ./internal/worker ./scripts

test-agent:
	$(GO) test -race -count=1 ./internal/executor/agent ./internal/eval/gateway ./internal/api ./internal/pgstore

smoke-agent-eval:
	$(GO) run ./cmd/orbit-agent-eval -fake-provider=true -repository ../gateway-system

smoke-agent:
	./scripts/smoke_agent.sh

test-mysql-foundation:
	cd experiments/mysql8 && $(GO) test -count=1 ./...

test-mysql-lab:
	cd experiments/mysql8 && $(GO) test -count=1 ./...

test-mysql-concurrency:
	cd experiments/mysql8 && $(GO) test -count=3 -run 'TestConcurrent|TestDeadlock|TestSkipLocked' ./...

report-mysql-explain:
	cd experiments/mysql8 && $(GO) test -count=1 -run TestExplainAnalyze -v ./...

smoke-phase5:
	./scripts/smoke_phase5.sh

smoke-llm:
	./scripts/smoke_llm.sh

demo:
	./scripts/demo.sh

verify: lint test-race build test-integration smoke-phase5 test-llm-executor smoke-llm test-agent smoke-agent smoke-agent-eval test-mysql-lab test-mysql-concurrency

lint:
	$(GO) vet ./...
	@test -z "$$($(GO)fmt -l .)" || (echo "go files need formatting"; $(GO)fmt -l .; exit 1)

fmt:
	$(GO)fmt -w .

tools:
	./scripts/install-tools.sh

proto:
	mkdir -p gen
	PATH=$(CURDIR)/bin:$$PATH ./bin/protoc -I proto --go_out=gen --go_opt=paths=source_relative --go-grpc_out=gen --go-grpc_opt=paths=source_relative proto/orbit/worker/v1/worker.proto

compose-up:
	$(COMPOSE) up -d postgres prometheus

compose-down:
	$(COMPOSE) down --remove-orphans

migrate-up:
	$(GO) run ./cmd/orbit-migrate up

migrate-down:
	$(GO) run ./cmd/orbit-migrate down 1

run-server:
	$(GO) run ./cmd/orbit-server
