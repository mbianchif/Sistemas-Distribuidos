FROM golang:1.24.2 AS builder

WORKDIR /analyzer/workers
COPY analyzer/workers/*.go    .
COPY analyzer/workers/config  ./config
COPY analyzer/workers/divider ./divider

WORKDIR /analyzer
COPY analyzer/comms                 ./comms
COPY analyzer/go.*                  ./
COPY analyzer/checker/impl/acker.go ./checker/impl/acker.go


RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/binary ./workers/divider/main.go

FROM busybox:1.37.0
COPY --from=builder /bin/binary /binary
ENTRYPOINT ["/binary"]
