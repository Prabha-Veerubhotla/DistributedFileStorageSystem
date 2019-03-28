#!/bin/bash

export SVR_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo $SVR_HOME
echo "** starting client from ${SVR_HOME} **"
echo "Enter ip address to connect with: "
read ipaddress

JAVA_MAIN='message.MessageClient'
JAVA_ARGS="$ipaddress"
#JAVA_ARGS="localhost"
JAVA_TUNE='-client -Xms512m -Xmx4g'

java ${JAVA_TUNE} -cp .:${SVR_HOME}/lib/'*':${SVR_HOME}/classes ${JAVA_MAIN} ${JAVA_ARGS} 
