MODULE := github.com/tradingmatchengine/trading_matchengine
BIN_DIR := bin

.PHONY: help test test-race cover lint tidy build clean migrate-up gen-proto

gen-proto:
	@bash scripts/gen-proto.sh

help:
	@echo "Targets:"
	@echo "  make gen-proto   - generate protobuf / gRPC code"
	@echo "  make test        - run all tests"
	@echo "  make test-race   - run tests with -race"
	@echo "  make cover       - test coverage report"
	@echo "  make tidy        - go mod tidy"
	@echo "  make build       - build cmd binaries (when added)"
	@echo "  make migrate-up  - apply SQL migrations (requires psql)"
	@echo "  make clean       - remove bin/"

test:
	go test ./...

test-integration:
	go test -tags=integration -count=1 -timeout 5m ./internal/order/integration/...

test-race:
	go test -race ./...

cover:
	go test -coverprofile=coverage.txt ./...
	go tool cover -html=coverage.txt -o coverage.html
	@echo "wrote coverage.html"

tidy:
	go mod tidy

build: gen-proto
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/matching ./cmd/matching
	go build -o $(BIN_DIR)/order ./cmd/order
	go build -o $(BIN_DIR)/gateway ./cmd/gateway
	go build -o $(BIN_DIR)/push ./cmd/push
	go build -o $(BIN_DIR)/kline ./cmd/kline
	go build -o $(BIN_DIR)/marketdata ./cmd/marketdata
	go build -o $(BIN_DIR)/indexprice ./cmd/indexprice

build-matching:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/matching ./cmd/matching

build-order:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/order ./cmd/order

build-gateway:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/gateway ./cmd/gateway

build-push:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/push ./cmd/push

build-kline:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/kline ./cmd/kline

build-marketdata:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/marketdata ./cmd/marketdata

build-indexprice: gen-proto
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/indexprice ./cmd/indexprice

clean:
	rm -rf $(BIN_DIR) coverage.txt coverage.html

migrate-up:
	@bash scripts/migrate-up.sh

# Optional: golangci-lint (install separately)
lint:
	@which golangci-lint >/dev/null || (echo "install: https://golangci-lint.run/welcome/install/" && exit 1)
	golangci-lint run ./...
