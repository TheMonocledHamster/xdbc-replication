.PHONY: test-postgres

test-postgres:
	XDBC_TEST_DATABASE_URL="postgres://postgres:postgres@localhost:$$(docker compose port postgres 5432 | sed 's/.*://')/xdbc_test?sslmode=disable" go test ./internal/postgres/...