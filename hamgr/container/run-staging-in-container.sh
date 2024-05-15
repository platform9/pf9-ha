#!/bin/bash

set -exo pipefail # in case of a command pipeline, allow non zero return code to be captured
[ -n "${BASH_DEBUG}" ] && set -x # setting BASH_DEBUG lets us debug shell bugs in this script

CONTAINER_BUILD_IMAGE=artifactory.platform9.horse/docker-local/py39-build-image:latest
THIS_FILE=$(realpath $0)
THIS_DIR=$(dirname ${THIS_FILE})
BUILD_UID=$(id -u)
BUILD_GID=$(id -g)

#trap 'sudo chown -R ${BUILD_UID}:${BUILD_GID} ${BUILD_DIR}' EXIT HUP INT QUIT ABRT TERM
echo ${THIS_DIR}

docker run -i --rm -a stdout -a stderr \
   -v ${THIS_DIR}/../../:/buildroot/pf9-ha \
   -e PF9_VERSION=${PF9_VERSION} \
   -e BUILD_NUMBER=${BUILD_NUMBER} \
   -e HOME=/buildroot/pf9-ha/build/.home \
   -u ${BUILD_UID}:${BUILD_GID} \
   ${CONTAINER_BUILD_IMAGE} \
   /buildroot/pf9-ha/hamgr/container/stage-with-container-build.sh
