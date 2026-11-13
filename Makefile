COMPOSE ?= $(if $(shell command -v docker 2>/dev/null),docker compose,podman compose)
DATASET ?= iot
COMPRESSION ?= snappy
TRANSFER_TIMEOUT ?= 1m
DATA_DIR ?= ./data
FILE ?= $(DATASET).csv
EXPORT_DIR ?= ./exports
SINK_TYPE ?= postgres
CSV_OUTPUT_PATH ?= /exports/$(DATASET).csv
LOCAL_UID ?= $(shell id -u)
LOCAL_GID ?= $(shell id -g)
export DATASET COMPRESSION DATA_DIR FILE TRANSFER_TIMEOUT SINK_TYPE CSV_OUTPUT_PATH LOCAL_UID LOCAL_GID

.PHONY: bench build up up-source up-target seed load fetch transfer verify export e2e e2e-all down test test-postgres

build:
	$(COMPOSE) build tools

up:
	mkdir -p exports
	$(COMPOSE) up -d --build --wait source-db target-db server client

up-source:
	$(COMPOSE) up -d --build --wait source-db server

up-target:
	mkdir -p exports
	$(COMPOSE) up -d --wait target-db
	$(COMPOSE) up -d --build --no-deps --wait client

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
	  $(MAKE) e2e DATASET=$$dataset; \
	done

# Fig. 9b benchmark (same as: docker compose run --build --rm bench). See bench/README.md.
# Example: make bench DATASETS=all JDBC=stream
bench:
	DATASETS="$(DATASETS)" JDBC="$(JDBC)" ROUNDS="$(ROUNDS)" SINKS="$(SINKS)" MACHINE="$(MACHINE)" \
	JDBC_HEAP_MB="$(JDBC_HEAP_MB)" QUERY_TIMEOUT="$(QUERY_TIMEOUT)" $(COMPOSE) run --build --rm bench

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
	go test -race -count=1 -timeout 60s ./internal/source/... ./internal/target/... ./internal/transfer/...
