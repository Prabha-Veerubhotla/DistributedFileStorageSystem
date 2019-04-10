#!/bin/bash

export SVR_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo $SVR_HOME

echo "** starting client from ${SVR_HOME} **"

echo "Enter ip address to connect with: "
read ipaddress

python3 python/MessageClient.py $ipaddress