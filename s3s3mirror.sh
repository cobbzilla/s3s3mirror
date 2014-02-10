#!/bin/bash

THISDIR=$(dirname $0)
cd ${THISDIR}
THISDIR=$(pwd)

VERSION=1.2.4
JARFILE=target/s3s3mirror-${VERSION}-SNAPSHOT.jar
VERSION_ARG="-Ds3s3mirror.version=${VERSION}"

DEBUG=$1
if [ "${DEBUG}" = "--debug" ] ; then
  # Run in debug mode
  shift   # remove --debug from options
  java -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005 ${VERSION_ARG} -jar ${JARFILE} "$@"

else
  # Run in regular mode
  java ${VERSION_ARG} -jar ${JARFILE} "$@"
fi

exit $?

