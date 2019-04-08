#!/bin/bash

export SVR_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo $SVR_HOME

echo "** starting client from ${SVR_HOME} **"

python3 src/message/MessageClient.py