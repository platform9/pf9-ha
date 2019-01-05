# Copyright (c) 2016 Platform9 Systems Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from sqlalchemy import Column
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import Text
from sqlalchemy import LargeBinary

meta = MetaData()


def upgrade(migrate_engine):
    meta.bind = migrate_engine
    # 'events' column in table 'change_events'
    table_change_events = Table('change_events', meta, autoload=True)
    table_change_events.c.events.alter(type=LargeBinary)
    # 'peers', 'members', 'kvstore' in table 'consul_status'
    table_consul_status = Table('consul_status', meta, autoload=True)
    table_consul_status.c.peers.alter(type=LargeBinary)
    table_consul_status.c.members.alter(type=LargeBinary)
    table_consul_status.c.kvstore.alter(type=LargeBinary)
    # 'before_rebalance', 'after_rebalance' in table 'consul_role_rebalance'
    table_consul_role_rebalance = Table('consul_role_rebalance', meta, autoload=True)
    table_consul_role_rebalance.c.before_rebalance.alter(type=LargeBinary)
    table_consul_role_rebalance.c.after_rebalance.alter(type=LargeBinary)

def downgrade(migrate_engine):
    meta.bind = migrate_engine
    # 'events' column in table 'change_events'
    table_change_events = Table('change_events', meta, autoload=True)
    table_change_events.c.events.alter(type=Text)
    # 'peers', 'members', 'kvstore' in table 'consul_status'
    table_consul_status = Table('consul_status', meta, autoload=True)
    table_consul_status.c.peers.alter(type=Text)
    table_consul_status.c.members.alter(type=Text)
    table_consul_status.c.kvstore.alter(type=Text)
    # 'before_rebalance', 'after_rebalance' in table 'consul_role_rebalance'
    table_consul_role_rebalance = Table('consul_role_rebalance', meta, autoload=True)
    table_consul_role_rebalance.c.before_rebalance.alter(type=Text)
    table_consul_role_rebalance.c.after_rebalance.alter(type=Text)
