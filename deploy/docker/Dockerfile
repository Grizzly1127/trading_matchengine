# 通用多阶段构建：docker build -f deploy/docker/Dockerfile --build-arg SERVICE=matching -t trading/matching:dev .
# syntax=docker/dockerfile:1

ARG GO_VERSION=1.26.3
FROM golang:${GO_VERSION}-alpine AS build
RUN apk add --no-cache ca-certificates git
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ARG SERVICE=matching
RUN test -n "${SERVICE}" && test -d "./cmd/${SERVICE}"
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/app "./cmd/${SERVICE}"

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata \
 && adduser -D -u 1000 -g app app
USER app
WORKDIR /app
COPY --from=build /out/app /usr/local/bin/trading-service
COPY configs /app/configs
ENTRYPOINT ["/usr/local/bin/trading-service"]
CMD ["-config", "/app/config/config.json"]
