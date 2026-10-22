COMPOSE ?= docker compose
COMPRESSION ?= none
export COMPRESSION

.PHONY: up seed transfer verify e2e down test test-postgres

up:
	$(COMPOSE) up -d --build --wait source-db target-db server client

seed:
	$(COMPOSE) run --rm --no-deps tools seed

transfer:
	$(COMPOSE) run --rm --no-deps tools transfer

verify:
	$(COMPOSE) run --rm --no-deps tools verify

e2e: up
	$(COMPOSE) run --rm --no-deps tools e2e

down:
	$(COMPOSE) down

test:
	go test -race -count=1 -timeout 60s ./...
	go vet ./...

test-postgres:
	$(COMPOSE) up -d --wait source-db target-db
	XDBC_TEST_DATABASE_URL="postgres://postgres:postgres@localhost:$(or $(SOURCE_POSTGRES_PORT),5433)/xdbc_test?sslmode=disable" \
	XDBC_TEST_SOURCE_DATABASE_URL="postgres://postgres:postgres@localhost:$(or $(SOURCE_POSTGRES_PORT),5433)/xdbc_test?sslmode=disable" \
	XDBC_TEST_TARGET_DATABASE_URL="postgres://postgres:postgres@localhost:$(or $(TARGET_POSTGRES_PORT),5434)/xdbc_test?sslmode=disable" \
	go test -race -count=1 -timeout 60s ./internal/postgres/... ./internal/target/... ./internal/transfer/...
