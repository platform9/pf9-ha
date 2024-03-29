#!/usr/bin/env sh
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
# the cryptography dependency requires higher version of pip and setuptools
# during tox install cryptography package, so customize tox virtualenv
# before install the dependencies
echo 'upgrade pip, setuptools, pbr...'
pip install -U pip==20.2.4 setuptools==57.1.0 pbr==3.1.1
echo 'install dependencies from ' + $@
pip install -cupper-constraints.txt $@
