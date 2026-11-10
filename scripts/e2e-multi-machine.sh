#!/bin/sh
# Two isolated Compose networks, connected only through published host ports.
set -eu

COMPOSE=${COMPOSE:-docker compose}
HOST_IP=${XDBC_TEST_HOST_IP:?Set XDBC_TEST_HOST_IP to a host IPv4 address reachable from containers}
SOURCE_PROJECT=xdbc-split-source-$$
TARGET_PROJECT=xdbc-split-target-$$
export DATASET=${DATASET:-iot} COMPRESSION=${COMPRESSION:-snappy} TRANSFER_TIMEOUT=2m
export SERVER_BIND_IP=$HOST_IP SOURCE_DB_BIND_IP=$HOST_IP
export SERVER_PORT=19000 SOURCE_POSTGRES_PORT=15433
export TARGET_POSTGRES_PORT=15434 CLIENT_PORT=18080
export SERVER_ADDR=$HOST_IP:$SERVER_PORT
# The source starts with its local Compose database. Only target tooling uses
# the remote source URL below.
unset SOURCE_DATABASE_URL TARGET_DATABASE_URL CLIENT_URL SERVER_URL

cleanup() {
    result=$?
    if [ "$result" -ne 0 ]; then
        $COMPOSE -p "$SOURCE_PROJECT" logs --no-color || true
        $COMPOSE -p "$TARGET_PROJECT" logs --no-color || true
    fi
    $COMPOSE -p "$TARGET_PROJECT" down -v || true
    $COMPOSE -p "$SOURCE_PROJECT" down -v || true
    exit "$result"
}
trap cleanup EXIT

make COMPOSE="$COMPOSE -p $SOURCE_PROJECT" up-source
export SOURCE_DATABASE_URL="postgres://postgres:postgres@$HOST_IP:$SOURCE_POSTGRES_PORT/xdbc_test?sslmode=disable"
make COMPOSE="$COMPOSE -p $TARGET_PROJECT" up-target

source_services=$($COMPOSE -p "$SOURCE_PROJECT" ps --services --status running | sort)
target_services=$($COMPOSE -p "$TARGET_PROJECT" ps --services --status running | sort)
[ "$source_services" = "$(printf 'server\nsource-db')" ]
[ "$target_services" = "$(printf 'client\ntarget-db')" ]

make COMPOSE="$COMPOSE -p $TARGET_PROJECT" seed transfer verify
# A second request verifies that the persistent processes remain reusable.
make COMPOSE="$COMPOSE -p $TARGET_PROJECT" transfer verify
