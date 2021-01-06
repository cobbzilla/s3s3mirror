#!/bin/bash


THISDIR=$(cd "$(dirname $0)" && pwd)

VERSION=2.1.3
JARFILE="${THISDIR}/target/s3s3mirror-${VERSION}-SNAPSHOT.jar"
VERSION_ARG="-Ds3s3mirror.version=${VERSION}"

TEMPFILE=$(mktemp /tmp/$(basename 0).XXXXXXX)

DEBUG=$1
if [ "${DEBUG}" = "--debug" ] ; then
  # Run in debug mode
  shift   # remove --debug from options
  java -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=6005 ${VERSION_ARG} -jar "${JARFILE}" "$@" 2>&1 | tee $TEMPFILE

else
  # Run in regular mode
  java ${VERSION_ARG} -jar "${JARFILE}" "$@" 2>&1 | tee $TEMPFILE
fi

# Check the output for errors - this is very crude and should be removed
# when #51 is resolved ( https://github.com/cobbzilla/s3s3mirror/issues/51 )
grep --quiet --ignore-case --max-count=1 'error ' $TEMPFILE
RESULT=$?
[ $RESULT -eq 0 ] && EXITCODE=1 || EXITCODE=0

# An empty variable will terminate script
set -o nounset

rm -f $TEMPFILE

exit $EXITCODE
