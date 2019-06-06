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
logfile=/var/log/pf9/pf9-ha-rpm-before-remove.log
touch "${logfile}"
echo "" > "${logfile}"
chown pf9:pf9group "${logfile}"

echo "begin of before-remove.sh ($(date '+%FT%T'))" >> "${logfile}"
echo "--------------------------" >> "${logfile}"

PIDFILE=/var/run/pf9-consul/pf9-consul.pid
pid=`cat ${PIDFILE}`

if ps -p ${pid} > /dev/null
then
    echo "kill -TERM ${pid}" >> "${logfile}"
    kill -TERM ${pid}
    echo "exit code : $?" >> "${logfile}"
    echo "" >> "${logfile}"
fi

files=(
    "${PIDFILE}"
    "/var/lock/subsys/pf9-consul"
    "/var/consul-status/last_update"
    "/opt/pf9/consul-data-dir"
    "/opt/pf9/etc/pf9-consul*"
    "/opt/pf9/etc/pf9-ha/conf.d/pf9-ha.conf"
    "/opt/pf9/etc/pf9-consul/conf.d/*.json"
    "/etc/logrotate.d/pf9-ha"
)

for file in ${files[@]}; do
    echo "rm -rf ${file}" >> "${logfile}"
    rm -rf "${file}"
    echo "exit code : $?" >> "${logfile}"
    echo "" >> "${logfile}"
done

echo "--------------------------" >> "${logfile}"
echo "end of before-remove.sh ($(date '+%FT%T'))" >> "${logfile}"