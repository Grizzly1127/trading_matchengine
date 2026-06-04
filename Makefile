MODULE := github.com/tradingmatchengine/trading_matchengine
BIN_DIR := bin

.PHONY: help test test-race cover lint tidy build clean migrate-up gen-proto \
	docker-build docker-build-matching docker-build-order docker-build-gateway \
	helm-template kustomize-build \
	bench bench-l0 bench-l0-smoke build-bench bench-l2

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
	@echo "  make bench-l0    - L0 micro benchmarks (engine, skiplist, wal)"
	@echo "  make bench-l0-smoke - short bench for CI"
	@echo "  make build-bench - build bench-producer, bench-report"
	@echo "  make bench-l2    - L2 matching load (requires dev.sh matching)"

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
	go build -o $(BIN_DIR)/auth ./cmd/auth
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

build-auth:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/auth ./cmd/auth

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

# --- Docker（deploy/docker/Dockerfile.*）---
DOCKER_SERVICES := matching order gateway auth push marketdata kline indexprice

docker-build-matching:
	docker build -f deploy/docker/Dockerfile.matching -t trading/matching:dev .

docker-build-order:
	docker build -f deploy/docker/Dockerfile.order -t trading/order:dev .

docker-build-gateway:
	docker build -f deploy/docker/Dockerfile.gateway -t trading/gateway:dev .

docker-build: docker-build-matching docker-build-order docker-build-gateway
	@for s in auth push marketdata kline indexprice; do \
		docker build -f deploy/docker/Dockerfile.$$s -t trading/$$s:dev .; \
	done

helm-template:
	helm template trading deploy/k8s/helm/trading-engine -n trading

kustomize-build:
	kubectl kustomize deploy/k8s/manifests

migrate-up:
	@bash scripts/migrate-up.sh

# Optional: golangci-lint (install separately)
lint:
	@which golangci-lint >/dev/null || (echo "install: https://golangci-lint.run/welcome/install/" && exit 1)
	golangci-lint run ./...

bench-l0:
	@chmod +x scripts/bench/run-l0.sh 2>/dev/null || true
	./scripts/bench/run-l0.sh

bench-l0-smoke:
	@chmod +x scripts/bench/run-l0.sh 2>/dev/null || true
	./scripts/bench/run-l0.sh --smoke

build-bench:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/bench-producer ./cmd/bench-producer
	go build -o $(BIN_DIR)/bench-report ./cmd/bench-report

bench: build-bench bench-l0

bench-l2: build-bench
	@chmod +x scripts/bench/*.sh 2>/dev/null || true
	./scripts/bench/run-l2.sh
