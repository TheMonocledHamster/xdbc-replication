.PHONY: test-postgres

test-postgres:
	XDBC_TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5433/xdbc_test?sslmode=disable" go test ./internal/postgres/...