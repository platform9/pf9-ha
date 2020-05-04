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

if [ $# -ne 1 ]
then
    echo "$0 requires one argument:"
    echo "   $1 -> root directory for the build"
    exit 1
fi

buildroot=$1
python=$buildroot/opt/pf9/python
mkdir -p $python

url=artifacts.platform9.horse/repository/yum-repo-frozen/hostagent-components/python3.6.7.tgz
wget -q -O- ${url} \
    | tar zxf - --strip-components=3 -C ${python}

