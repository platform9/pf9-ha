#!/bin/bash

set -e
set -x

if [ $# -ne 5 ]; then
    echo "$0 requires 5 arguments:"
    echo "   $1 -> version string major.minor"
    echo "   $2 -> buildnumber"
    echo "   $3 -> source dir"
    echo "   $4 -> output dir"
    echo "   $5 -> deb or rpm"
    exit 1
fi

version=$1
buildnum=$2
srcdir=`cd $3; pwd`
outdir=`cd $4; pwd`
pkgtype=$5
thisdir=`cd $(dirname $0); pwd`

echo "### version=${version} , buildnum=${buildnum} , srcdir=${srcdir} , outdir=${outdir} , pkgtype=${pkgtype} , thisdir=${thisdir}"

name=pf9-ha-slave
githash=`git rev-parse --short HEAD`
arch=x86_64
desc="Platform9 On-Host consul cluster manager service from pf9-ha@$githash"
package=$outdir/$name-$version-$buildnum.$arch.$pkgtype

[ -e $package ] && rm -f $package

mkdir -p $outdir/fpm-work

echo $srcdir
echo $package
echo $outdir

fpm -t $pkgtype -s dir -n $name \
    --workdir $outdir/fpm-work \
    --description "$desc" \
    --license "Commercial" \
    --architecture $arch \
    --url "http://www.platform9.com" \
    --vendor Platform9 \
    --version $version \
    --iteration $buildnum \
    --provides $name \
    --provides pf9app \
    --depends pf9-bbslave \
    --exclude "**opt/pf9/python**" \
    --package $package \
    --after-install $thisdir/after-install.sh \
    --before-remove $thisdir/before-remove.sh \
    -C $srcdir .


case $pkgtype in
    rpm)
        ;;
    deb)
        ;;
    *)
        ;;
esac

