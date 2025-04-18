FROM golang:1.24.2 AS builder

WORKDIR /app
COPY . .
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/binary main.go

FROM busybox:latest
COPY --from=builder /app/bin/binary /binary
ENTRYPOINT ["/binary"]