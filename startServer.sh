#!/bin/bash
#
# This script is used to start the server from a supplied config file
#

if [ "$#" -ne 1 ]; then
   echo -e "\nUsage: $0 <config file>\n"
   exit
fi


export SVR_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo "** starting server from ${SVR_HOME} **"

echo server home = $SVR_HOME

JAVA_MAIN='grpc.route.server.RouteServerImpl'
JAVA_ARGS="$1"
echo -e "\n** config: ${JAVA_ARGS} **\n"

# superceded by http://www.oracle.com/technetwork/java/tuning-139912.html
JAVA_TUNE='-server -Xms500m -Xmx1000m'


java ${JAVA_TUNE} -cp .:${SVR_HOME}/lib/'*':${SVR_HOME}/classes ${JAVA_MAIN} ${JAVA_ARGS} 
