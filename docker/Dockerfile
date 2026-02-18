FROM golang:1.26-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/crawler ./cmd/crawler
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/parser ./cmd/parser
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/migrate ./cmd/migrate
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/seeder ./cmd/seeder

FROM alpine:3.21

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /out/ .
COPY configs/ ./configs/
COPY seeds.txt ./seeds.txt
COPY internal/database/migrations/ ./migrations/
