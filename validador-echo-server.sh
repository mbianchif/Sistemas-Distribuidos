#!/bin/sh

retrieve_from_server_config() {
    grep $1 server/config.ini | cut -d'=' -f2 | xargs
}

HOST=$(retrieve_from_server_config "SERVER_IP")
PORT=$(retrieve_from_server_config "SERVER_PORT")
MSG="Hello Server!"

RESPONSE=$(docker run --rm --network=tp0_testing_net --entrypoint sh subfuzion/netcat -c "echo \"$MSG\" | nc -w 2 $HOST $PORT")

if [ "$RESPONSE" = "$MSG" ]; then
    echo "action: test_echo_server | result: success"
else
    echo "action: test_echo_server | result: fail"
fi
