FROM golang:1.24.2 AS builder

# Install Docker
RUN apt-get update && apt-get install -y \
    curl \
    && curl -fsSL https://get.docker.com | sh

WORKDIR /analyzer/checker
COPY analyzer/checker/. .

WORKDIR /analyzer
COPY analyzer/go.* ./

RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o binary checker/main.go

FROM busybox:1.37.0
COPY --from=builder /analyzer/binary /binary
COPY --from=builder /usr/bin/docker  /usr/bin/docker
ENTRYPOINT ["/binary"]
