.PHONY: build test test-race lint fmt compose-up compose-down migrate-up migrate-down run-server

GO ?= go
COMPOSE := docker compose -f deploy/docker-compose.yml

build:
	$(GO) build ./cmd/...

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

lint:
	$(GO) vet ./...
	@test -z "$$($(GO)fmt -l .)" || (echo "go files need formatting"; $(GO)fmt -l .; exit 1)

fmt:
	$(GO)fmt -w .

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

