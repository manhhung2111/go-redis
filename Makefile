.PHONY: clean generate run-test run-test-race run-test-cover-command run-test-cover-storage run-test-cover-ds run-test-cover-core  run-server

clean:
	go clean -cache -testcache

generate:
	wire ./internal/wiring

run-test: clean generate
	go test ./... -v

run-test-race: clean
	go test ./... -race

run-test-cover-command: clean
	go test ./... \
		-coverpkg=./internal/command \
		-coverprofile=coverage-command.out
	go tool cover -func=coverage-command.out

run-test-cover-storage: clean
	go test ./... \
		-coverpkg=./internal/storage \
		-coverprofile=coverage-storage.out
	go tool cover -func=coverage-storage.out

run-test-cover-ds: clean
	go test ./... \
		-coverpkg=./internal/storage/data_structure \
		-coverprofile=coverage-ds.out
	go tool cover -func=coverage-ds.out

run-test-cover-core: clean
	go test ./... \
		-coverpkg=./internal/core \
		-coverprofile=coverage-core.out
	go tool cover -func=coverage-core.out

run-server: generate
	go run ./cmd/main.go