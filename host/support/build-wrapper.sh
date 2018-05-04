#!/bin/bash

set -e
set -x

if [ $# -ne 4 ]; then
    echo "$0 requries 3 arguments:"
    echo "   $1 -> version string major.minor"
    echo "   $2 -> buildnumber"
    echo "   $3 -> build dir containing the RPMs"
    echo "   $4 -> staging directory"
    exit 1
fi

version=$1
buildnum=$2
builddir=$3
stagedir=$4
thisdir=`cd $(dirname $0); pwd`

name=pf9-ha-slave-wrapper
arch=x86_64
desc="Wrapper package for Platform9 On-host consul cluster manager service"
package=$builddir/$name-$version-$buildnum.$arch.rpm

[ -e $package ] && rm -f $package

mkdir -p $builddir
mkdir -p $stagedir
mkdir -p $builddir/fpm-work

# Make the correct directory structure as we would expect in DU
install -d -m 755 ${stagedir}/opt/pf9/www/private
install -d -m 755 ${stagedir}/etc/pf9/resmgr_roles/pf9-ha-slave/${version}/
install -d -m 755 ${stagedir}/etc/pf9/resmgr_roles/conf.d/

install -p -m 644 ${builddir}/pf9-ha-slave*.rpm ${stagedir}/opt/pf9/www/private
install -p -m 644 ${builddir}/pf9-ha-slave*.deb ${stagedir}/opt/pf9/www/private
install -p -m 644 ${thisdir}/../ha/pf9app/pf9-ha-role.json \
    ${stagedir}/etc/pf9/resmgr_roles/pf9-ha-slave/${version}/pf9-ha-role.json
install -p -m 644 ${thisdir}/ha-slave-resmgr.conf \
    ${stagedir}/etc/pf9/resmgr_roles/conf.d/

rpm_name=`ls ${builddir}/pf9-ha-slave*.rpm`
rpm_name=`basename ${rpm_name}`

sed -i "s/__RPMNAME__/${rpm_name}/" ${stagedir}/etc/pf9/resmgr_roles/pf9-ha-slave/${version}/pf9-ha-role.json
sed -i "s/__VERSION__/${version}-${buildnum}/" ${stagedir}/etc/pf9/resmgr_roles/pf9-ha-slave/${version}/pf9-ha-role.json

fpm -t rpm -s dir -n $name \
    --workdir ${builddir}/fpm-work \
    --description "$desc" \
    --license "Commercial" \
    --architecture $arch \
    --url "http://platform9.com" \
    --vendor Platform9 \
    --iteration $buildnum \
    --version $version \
    --provides pf9-ha-slave-wrapper \
    --package $package \
    -C ${stagedir} .

