#!/bin/bash

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

srcroot=$(cd $(dirname $buildroot)/../host; pwd)
echo "SRCROOT = $srcroot"

# virtualenv and setup
virtualenv -p $python ${buildroot}/opt/pf9/pf9-ha
prelink -u ${buildroot}/opt/pf9/pf9-ha/bin/python || true
pushd ${srcroot}
PBR_VERSION=3.1.1 \
${buildroot}/opt/pf9/pf9-ha/bin/python \
    ${srcroot}/setup.py install

PBR_VERSION=3.1.1 \
PIP_CACHE_DIR=~/.cache/pip-py27-netsvc ${buildroot}/opt/pf9/pf9-ha/bin/python \
    ${buildroot}/opt/pf9/pf9-ha/bin/pip install .

popd

# patch the #!python with the venv's python
sed -i "s/\#\!.*python/\#\!\/opt\/pf9\/pf9-ha\/bin\/python/" \
           ${buildroot}/opt/pf9/pf9-ha/bin/*

# Create required directories
install -d -m 755 ${buildroot}/opt/pf9/etc/pf9-consul/conf.d
install -d -m 755 ${buildroot}/opt/pf9/etc/pf9-ha/conf.d
install -d -m 755 ${buildroot}/usr/bin
install -d -m 755 ${buildroot}/etc/init.d/
install -d -m 755 ${buildroot}/opt/pf9/pf9-ha-slave
install -d -m 755 ${buildroot}/var/consul-status


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

