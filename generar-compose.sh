#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Use: $0 <output-file-name> <n-clients>"
    exit 1
fi

# header
echo "name: tp0
services:" > $1

# server
echo "  server:
    container_name: server
    image: server:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - LOGGING_LEVEL=DEBUG
    networks:
      - testing_net
    volumes:
      - ./server/config.ini:/config.ini:ro
" >> $1

# clients
for i in $(seq 1 $2);
do
  echo "  client$i:
    container_name: client$i
    image: client:latest
    entrypoint: /client
    environment:
      - CLI_ID=$i
      - CLI_LOG_LEVEL=DEBUG
    networks:
      - testing_net
    depends_on:
      - server
    volumes:
      - ./client/config.yaml:/config.yaml:ro
" >> $1
done

# networks
echo "networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24" >> $1
