#!/bin/bash -x

set -e

export PBR_VERSION=3.1.1

proj=hamgr
githash=${PF9_GITHASH:-`git rev-parse --short HEAD`}
echo "** hamgr git hash : ${githash}"
version=${PF9_VERSION:-1.5.0}
echo "** hamgr version : ${version}"
echo "** hamgr build : ${PF9_BUILD_NUMBER}"
spec=pf9-$proj.spec
srcroot=$(dirname $(readlink -f $0))/..
echo "### srcroot = ${srcroot}"
rpmbuild=$srcroot/../build/$proj
echo "### rpmbuild = ${rpmbuild}"
shared=$(readlink -f ../../shared)
echo "### shared = ${shared}"


package=pf9-hamgr
svcuser=hamgr
svcgroup=hamgr
release=${PF9_BUILD_NUMBER:-0}.${githash}

# build rpm environment
[ -d $rpmbuild ] && rm -rf $srcroot/rpmbuild
for i in BUILD RPMS SOURCES SPECS SRPMS tmp
do
    mkdir -p $rpmbuild/${i}
done
cp -f $spec $rpmbuild/SPECS/

# build a source tarball
cd ../
cp -rf $proj $rpmbuild/SOURCES/
cp -rf ${shared} $rpmbuild/SOURCES/
cp -rf etc $rpmbuild/SOURCES/
cp -rf bin $rpmbuild/SOURCES/
cp -f setup.py $rpmbuild/SOURCES/
cp -f setup.cfg $rpmbuild/SOURCES/
cp -f requirements.txt $rpmbuild/SOURCES/
cp -f README.md $rpmbuild/SOURCES/

cd $rpmbuild/SOURCES/
tar -c --exclude='*.pyc' -f source.tar \
        $proj \
        shared \
        etc \
        bin \
        setup.py \
        setup.cfg \
        requirements.txt \
        README.md

cd ${srcroot}

# make sure PYTHONPATH is unset so we don't accidentally satisfy a dependency
# using a package outside the virtualenv
unset PYTHONPATH

# QA_SKIP_BUILD_ROOT is added to skip a check in rpmbuild that greps for
# the buildroot path in the installed binaries. Many of the python
# binary extension .so libraries do this.
QA_SKIP_BUILD_ROOT=1 rpmbuild -ba \
         --define "_topdir $rpmbuild"  \
         --define "_tmpdir $rpmbuild/tmp" \
         --define "_package $package" \
         --define "_version $version"  \
         --define "_release $release" \
         --define "_githash $githash" \
         --define "_svcuser $svcuser" \
         --define "_svcgroup $svcgroup" \
         --define "_gpg_name dev@platform9.net" \
         support/$spec

${srcroot}/support/sign_packages.sh ${rpmbuild}/RPMS/*/*$proj*.rpm

