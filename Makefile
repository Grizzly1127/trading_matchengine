MODULE := github.com/tradingmatchengine/trading_matchengine
BIN_DIR := bin

.PHONY: help test test-race cover lint tidy build clean migrate-up

help:
	@echo "Targets:"
	@echo "  make test        - run all tests"
	@echo "  make test-race   - run tests with -race"
	@echo "  make cover       - test coverage report"
	@echo "  make tidy        - go mod tidy"
	@echo "  make build       - build cmd binaries (when added)"
	@echo "  make migrate-up  - apply SQL migrations (requires psql)"
	@echo "  make clean       - remove bin/"

test:
	go test ./...

test-race:
	go test -race ./...

cover:
	go test -coverprofile=coverage.txt ./...
	go tool cover -html=coverage.txt -o coverage.html
	@echo "wrote coverage.html"

tidy:
	go mod tidy

build:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/matching ./cmd/matching
	go build -o $(BIN_DIR)/order ./cmd/order
	go build -o $(BIN_DIR)/gateway ./cmd/gateway

build-matching:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/matching ./cmd/matching

build-order:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/order ./cmd/order

build-gateway:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/gateway ./cmd/gateway

clean:
	rm -rf $(BIN_DIR) coverage.txt coverage.html

migrate-up:
	@bash scripts/migrate-up.sh

# Optional: golangci-lint (install separately)
lint:
	@which golangci-lint >/dev/null || (echo "install: https://golangci-lint.run/welcome/install/" && exit 1)
	golangci-lint run ./...
