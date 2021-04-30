#!/bin/bash
# Copyright (c) 2019 Platform9 Systems Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e
set -x

if [ "$#" -ne 3 ]
then
    echo "$0 requires three arguments:"
    echo '   $1 -> root directory for the build'
    echo '   $2 -> python binary to use for the virtualenv'
    echo '   $3 -> system type: debian or redhat'
    exit 1
else
    buildroot=`cd $1; pwd`
    python=$2
    systemtype=$3
    mkdir -p $buildroot
fi

echo "buildroot=${buildroot}"

srcroot=$(cd $(dirname $buildroot)/../../host; pwd)
echo "SRCROOT = $srcroot"
shared=$(cd $(dirname $buildroot)/../../shared; pwd)

# virtualenv and setup
virtualenv -p $python ${buildroot}/opt/pf9/pf9-ha
prelink -u ${buildroot}/opt/pf9/pf9-ha/bin/python || true

# copy to tmp folder
mkdir -p $buildroot/tmp
cp -rf ${srcroot}/ha $buildroot/tmp/
cp -rf ${shared} $buildroot/tmp/
cp -f ${srcroot}/README.md $buildroot/tmp/
cp -f ${srcroot}/requirements.txt $buildroot/tmp/
cp -f ${srcroot}/setup.cfg $buildroot/tmp/
cp -f ${srcroot}/setup.py $buildroot/tmp/

# remove tests
rm -rf $buildroot/tmp/shared/tests

pushd $buildroot/tmp
PIP_CACHE_DIR=~/.cache/pip-py36-netsvc ${buildroot}/opt/pf9/pf9-ha/bin/python \
    ${buildroot}/opt/pf9/pf9-ha/bin/pip install --upgrade pip==20.2.4 pbr==3.1.1 setuptools==33.1.1

PBR_VERSION=3.1.1 ${buildroot}/opt/pf9/pf9-ha/bin/python ${srcroot}/setup.py install

PBR_VERSION=3.1.1 \
PIP_CACHE_DIR=~/.cache/pip-py36-netsvc ${buildroot}/opt/pf9/pf9-ha/bin/python \
    ${buildroot}/opt/pf9/pf9-ha/bin/pip install -c https://raw.githubusercontent.com/openstack/requirements/stable/rocky/upper-constraints.txt .

# Following should be removed when pf9-ha is upgraded to stable/stein
PIP_CACHE_DIR=~/.cache/pip-py36-netsvc ${buildroot}/opt/pf9/pf9-ha/bin/python \
    ${buildroot}/opt/pf9/pf9-ha/bin/pip install eventlet==0.24.1
popd

# patch the #!python with the venv's python
sed -i "s/\#\!.*python/\#\!\/opt\/pf9\/pf9-ha\/bin\/python/" \
           ${buildroot}/opt/pf9/pf9-ha/bin/*
sed -i "s/exec' .*python/exec' \/opt\/pf9\/pf9-ha\/bin\/python/" \
    ${buildroot}/opt/pf9/pf9-ha/bin/*

# Create required directories
install -d -m 755 ${buildroot}/opt/pf9/etc/pf9-consul/conf.d
install -d -m 755 ${buildroot}/opt/pf9/etc/pf9-ha/conf.d
install -d -m 755 ${buildroot}/usr/bin
install -d -m 755 ${buildroot}/etc/init.d/
install -d -m 755 ${buildroot}/opt/pf9/pf9-ha-slave
install -d -m 755 ${buildroot}/var/consul-status
install -d -m 755 ${buildroot}/opt/pf9/support

# Copy init files specific to OS
if [ "x$systemtype" = "xrpm" ]; then

    install -p -m 755 ${srcroot}/ha/pf9app/etc/init.d/pf9-ha-slave-redhat \
        ${buildroot}/etc/init.d/pf9-ha-slave
    install -p -m 755 ${srcroot}/ha/pf9app/etc/init.d/pf9-consul-redhat \
        ${buildroot}/etc/init.d/pf9-consul
else

    install -p -m 755 ${srcroot}/ha/pf9app/etc/init.d/pf9-ha-slave-debian \
        ${buildroot}/etc/init.d/pf9-ha-slave
    install -p -m 755 ${srcroot}/ha/pf9app/etc/init.d/pf9-consul-debian \
        ${buildroot}/etc/init.d/pf9-consul
fi


# Copy files
install -p -m 755 -t ${buildroot}/usr/bin \
   ${srcroot}/ha/pf9app/opt/pf9/pf9-consul/consul
install -p -m 644 -t ${buildroot}/opt/pf9/etc/pf9-consul/ \
    ${srcroot}/ha/pf9app/opt/pf9/etc/pf9-consul/conf.d/*
install -p -m 755 -t ${buildroot}/opt/pf9/pf9-ha-slave/ \
    ${srcroot}/ha/pf9app/config

# install support script
install -p -D ${srcroot}/support/pf9_consul_dump.sh ${buildroot}/opt/pf9/support/
install -p -D ${srcroot}/support/pf9_ha_config_dump.sh ${buildroot}/opt/pf9/support/

# install the /etc/logrotate.d/
install -p -m 644 -D ${srcroot}/ha/pf9app/etc/logrotate.pf9-ha \
                  ${buildroot}/etc/logrotate.d/pf9-ha

