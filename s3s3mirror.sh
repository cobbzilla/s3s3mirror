#!/bin/bash

THISDIR=$(dirname $0)
cd ${THISDIR}
THISDIR=$(pwd)

DEBUG=$1
if [ "x${DEBUG}" = "x" ] ; then
  # This will cause an error, required args missing, but let the code tell them what the problem is
  java -jar target/s3s3mirror-1.0.0-SNAPSHOT.jar "$@"

elif [ "${DEBUG}" = "--debug" ] ; then
  # Run in debug mode
  shift   # remove --debug from options
  java -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005 -jar target/s3s3mirror-1.0.0-SNAPSHOT.jar "$@"

else
  # Run in regular mode
  java -jar target/s3s3mirror-1.0.0-SNAPSHOT.jar "$@"
fi

exit $?

