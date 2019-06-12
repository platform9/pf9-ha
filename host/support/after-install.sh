#!/usr/bin/env bash
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
logfile=/var/log/pf9/pf9-ha-rpm-after-install.log
touch "${logfile}"
echo "" > "${logfile}"
chown pf9:pf9group "${logfile}"

echo "begin of after-install.sh ($(date '+%FT%T'))" >> "${logfile}"
echo "--------------------------" >> "${logfile}"
folders=(
    "/opt/pf9/pf9-ha"
    "/opt/pf9/pf9-ha-slave"
    "/opt/pf9/consul-data-dir"
    "/opt/pf9/etc/pf9-consul"
    "/opt/pf9/etc/pf9-ha"
    "/var/run/pf9-consul/"
    "/var/run/pf9-ha/"
    "/var/consul-status/"
)

for folder in ${folders[@]}; do
    echo "mkdir ${folder}" >> "${logfile}"
    mkdir -p "${folder}"
    echo "exit code : $?" >> "${logfile}"
    echo "" >> "${logfile}"
    stat "${folder}" >> "${logfile}"
    echo "" >> "${logfile}"
    echo "chown -R pf9:pf9group ${folder}" >> "${logfile}"
    chown -R pf9:pf9group "${folder}"
    echo "exit code : $?" >> "${logfile}"
    echo "" >> "${logfile}"
    stat "${folder}" >> "${logfile}"
    echo "" >> "${logfile}"
done
echo "--------------------------" >> "${logfile}"
echo "end of after-install.sh ($(date '+%FT%T'))" >> "${logfile}"
