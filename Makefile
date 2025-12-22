.PHONY: generate run-test run-test-race run-test-cover run-server

generate:
	wire ./internal/wiring

run-test: generate
	go test ./... -v

run-test-race:
	go test ./... -race

run-test-cover:
	go test ./... -cover

run-server:
	go run ./cmd/main.go