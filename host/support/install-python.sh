#!/bin/bash

set -e
set -x

if [ $# -ne 1 ]
then
    echo "$0 requires one argument:"
    echo "   $1 -> root directory for the build"
    exit 1
fi

buildroot=$1
python=$buildroot/opt/pf9/python
mkdir -p $python

url=netsvc/yum-repo-frozen/hostagent-components/python.tgz
wget -q -O- ${url} \
    | tar zxf - --strip-components=3 -C ${python}

