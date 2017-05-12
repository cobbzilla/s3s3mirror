#!/bin/bash

THISDIR=$(cd "$(dirname $0)" && pwd)

VERSION=1.2.6
JARFILE="${THISDIR}/target/s3s3mirror-${VERSION}-SNAPSHOT.jar"
VERSION_ARG="-Ds3s3mirror.version=${VERSION}"

DEBUG=$1
if [ "${DEBUG}" = "--debug" ] ; then
  # Run in debug mode
  shift   # remove --debug from options
  java -Dlog4j.configuration=file:target/classes/log4j.xml -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005 ${VERSION_ARG} -jar "${JARFILE}" "$@"

else
  # Run in regular mode
  java ${VERSION_ARG} -Dlog4j.configuration=file:target/classes/log4j.xml -jar "${JARFILE}" "$@"
fi

exit $?

