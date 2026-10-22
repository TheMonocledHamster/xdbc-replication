COMPOSE ?= docker compose
DATASET ?= iot
COMPRESSION ?= none
TRANSFER_TIMEOUT ?= 1m
DATA_DIR ?= ./data
FILE ?= $(DATASET).csv
EXPORT_DIR ?= ./exports
export DATASET COMPRESSION DATA_DIR FILE TRANSFER_TIMEOUT

.PHONY: build up seed load fetch transfer verify export e2e e2e-all down test test-postgres

build:
	$(COMPOSE) build tools

up:
	$(COMPOSE) up -d --build --wait source-db target-db server client

seed:
	$(COMPOSE) run --rm --no-deps tools seed

load:
	$(COMPOSE) run --rm --no-deps tools load

fetch: build
	mkdir -p "$(DATA_DIR)"
	$(COMPOSE) run --rm --no-deps tools fetch > "$(DATA_DIR)/$(FILE).part"
	mv "$(DATA_DIR)/$(FILE).part" "$(DATA_DIR)/$(FILE)"

transfer:
	$(COMPOSE) run --rm --no-deps tools transfer

verify:
	$(COMPOSE) run --rm --no-deps tools verify

export:
	mkdir -p "$(EXPORT_DIR)"
	$(COMPOSE) run --rm --no-deps tools export > "$(EXPORT_DIR)/$(DATASET).csv.part"
	mv "$(EXPORT_DIR)/$(DATASET).csv.part" "$(EXPORT_DIR)/$(DATASET).csv"

e2e: up
	$(COMPOSE) run --rm --no-deps tools e2e

e2e-all:
	@set -e; for dataset in lineitem acs iot icu; do \
	  for compression in none snappy; do \
	    $(MAKE) e2e DATASET=$$dataset COMPRESSION=$$compression; \
	  done; \
	done

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
