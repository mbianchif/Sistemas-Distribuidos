FROM golang:1.24.2 AS builder

WORKDIR /workers
COPY config ./config
COPY protocol ./protocol
COPY rabbit ./rabbit
COPY go.mod go.sum ./
COPY worker.go .

# Sink specific
COPY sink ./sink

RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/binary ./sink/main.go

FROM busybox:latest
COPY --from=builder /workers/bin/binary /binary
ENTRYPOINT ["/binary"]
