FROM golang:1.24.2 AS builder

WORKDIR /app
COPY . .
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/client main.go

FROM busybox:latest
COPY --from=builder /app/bin/client /client
ENTRYPOINT ["/client"]
