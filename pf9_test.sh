#!/bin/bash
set -eo pipefail # in case of a command pipeline, allow non zero return code to be captured
[ -n "${BASH_DEBUG}" ] && set -x # setting BASH_DEBUG lets us debug shell bugs in this script

REPO_DIR=$(cd $(dirname $0); git rev-parse --show-toplevel)
WORKSPACE=$(cd ${REPO_DIR}/..; pwd)
BUILD_DIR=${REPO_DIR}/build

BUILD_IMAGE=artifactory.platform9.horse/docker-local/py39-build-image:latest # can change to 5.6.0 if need be

#echo Running tox unit tests ...
#docker run --rm -i \
#    -u $(id -u):$(id -g) \
#    -v ${WORKSPACE}:/src \
#    -w /src/pf9-ha \
#    ${BUILD_IMAGE} \
#    tox -v --recreate -e py39
