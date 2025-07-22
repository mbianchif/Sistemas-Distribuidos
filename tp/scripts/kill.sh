#!/bin/bash

if [ "$#" -eq 0 ]; then
  echo "Usage: $0 <container1> [container2 ...]"
  exit 1
fi

for container in "$@"; do
  echo "Killing container: $container"
  docker kill --signal=9 "$container"
done
