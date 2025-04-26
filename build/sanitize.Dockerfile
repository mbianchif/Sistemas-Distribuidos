FROM golang:1.24.2 AS builder

WORKDIR /analyzer/workers
COPY analyzer/workers/worker.go .
COPY analyzer/workers/config    ./config
COPY analyzer/workers/sanitize  ./sanitize

WORKDIR /analyzer
COPY analyzer/comms ./comms
COPY analyzer/go.*  ./

RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/binary ./workers/sanitize/main.go

FROM busybox:1.37.0
COPY --from=builder /bin/binary /binary
ENTRYPOINT ["/binary"]
