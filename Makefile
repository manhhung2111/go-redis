.PHONY: clean generate run-test run-test-race run-test-cover  run-server

clean:
	go clean -cache -testcache

generate:
	wire ./internal/wiring

run-test: clean generate
	go test ./... -v

run-test-race: clean
	go test ./... -race

run-test-cover: clean
	go test ./... -cover

run-server: generate
	go run ./cmd/main.go