.PHONY: build test test-race test-integration smoke-phase5 lint fmt tools proto compose-up compose-down migrate-up migrate-down run-server

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

smoke-phase5:
	./scripts/smoke_phase5.sh

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
	$(COMPOSE) up -d postgres kafka prometheus

compose-down:
	$(COMPOSE) down --remove-orphans

migrate-up:
	$(GO) run ./cmd/orbit-migrate up

migrate-down:
	$(GO) run ./cmd/orbit-migrate down 1

run-server:
	$(GO) run ./cmd/orbit-server
