#!/bin/bash -x

set -e

export PBR_VERSION=1.8.1

proj=hamgr
githash=`git rev-parse --short HEAD`
version=${PF9_VERSION:-1.5.0}
spec=pf9-$proj.spec
srcroot=$(dirname $(readlink -f $0))/..
rpmbuild=$srcroot/../build/$proj
echo $rpmbuild

package=pf9-hamgr
svcuser=hamgr
svcgroup=hamgr
release=${BUILD_NUMBER:-0}.${githash}

# build rpm environment
[ -d $rpmbuild ] && rm -rf $srcroot/rpmbuild
for i in BUILD RPMS SOURCES SPECS SRPMS tmp
do
    mkdir -p $rpmbuild/${i}
done
cp -f $spec $rpmbuild/SPECS/

# build a source tarball
cd ../ && tar -c --exclude='*.pyc' -f $rpmbuild/SOURCES/source.tar \
        $proj \
        etc \
        bin \
        setup.py \
        setup.cfg \
        requirements.txt \
        README.md

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

