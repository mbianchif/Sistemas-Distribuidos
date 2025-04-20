FROM golang:1.24.2 AS builder

WORKDIR /workers
COPY config ./config
COPY protocol ./protocol
COPY rabbit ./rabbit
COPY go.mod go.sum ./
COPY worker.go .

# GroupBy specific
COPY groupby ./groupby

RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/binary ./groupby/main.go

FROM busybox:latest
COPY --from=builder /workers/bin/binary /binary
ENTRYPOINT ["/binary"]
