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

PIDFILE=/var/run/pf9-consul/pf9-consul.pid
LOCKFILE=/var/lock/subsys/pf9-consul
DATADIR=/opt/pf9/consul-data-dir
HA_CONF=/opt/pf9/etc/pf9-ha/conf.d/pf9-ha.conf
CONSUL_CONF=/opt/pf9/etc/pf9-consul/conf.d/*.json
pid=`cat ${PIDFILE}`
kill -TERM ${pid}
rm -f ${PIDFILE}
rm -rf ${DATADIR}
rm -f ${HA_CONF}
rm -f ${CONSUL_CONF}
rm -f ${LOCKFILE}
rm -f /var/consul-status/last_update
rm -rf /opt/pf9/etc/pf9-consul*
rm -rf /etc/logrotate.d/ha