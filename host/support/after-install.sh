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

mkdir -p /opt/pf9/consul-data-dir
mkdir -p /var/run/pf9-consul/
mkdir -p /var/run/pf9-ha/
mkdir -p /var/consul-status/
chown -R pf9:pf9group /var/run/pf9-consul
chown -R pf9:pf9group /var/run/pf9-ha
chown -R pf9:pf9group /opt/pf9/consul-data-dir
chown -R pf9:pf9group /opt/pf9/pf9-ha
chown -R pf9:pf9group /opt/pf9/pf9-ha-slave
chown -R pf9:pf9group /opt/pf9/etc/pf9-consul
chown -R pf9:pf9group /opt/pf9/etc/pf9-ha
chown -R pf9:pf9group /var/consul-status
