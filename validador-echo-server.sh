#!/bin/sh

retrieve_from_server_config() {
    grep $1 server/config.ini | cut -d'=' -f2 | xargs
}

HOST=$(retrieve_from_server_config "SERVER_IP")
PORT=$(retrieve_from_server_config "SERVER_PORT")
msg="Hello Server!"

response=$(echo msg | netcat $HOST $PORT)

if [ $response = $msg ]; then
    echo "action: test_echo_server | result: success"
else
    echo "action: test_echo_server | result: fail"
fi
