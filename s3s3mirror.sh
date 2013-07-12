#!/bin/bash

THISDIR=$(dirname $0)
cd ${THISDIR}
THISDIR=$(pwd)

JARFILE=target/s3s3mirror-1.0.0-SNAPSHOT.jar

DEBUG=$1
if [ "${DEBUG}" = "--debug" ] ; then
  # Run in debug mode
  shift   # remove --debug from options
  java -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005 -jar ${JARFILE} "$@"

else
  # Run in regular mode
  java -jar ${JARFILE} "$@"
fi

exit $?

