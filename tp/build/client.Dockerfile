FROM golang:1.24.2 AS builder

WORKDIR /analyzer/client
COPY analyzer/client/. .

WORKDIR /analyzer
COPY analyzer/go.* ./

RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o binary client/main.go

FROM busybox:1.37.0
COPY --from=builder /analyzer/binary /binary
ENTRYPOINT ["/binary"]
