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

from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Text

meta = MetaData()

# table used to track consul member auto rebalance processing, it will be triggered when there is
# host-up or host-down event received by hamgr, and the event has been handled with 'finished' status.
# it should looks like
#  {
#   'id':1,
#   'uuid': '' , # identifier of the rebalance action
#   'event_name':'host-up',
#   'event_uuid' : '',   # foreign key to 'events_processing' table for full picture of the event
#                        # also foreign key to 'consul_status' table for full picture of the consul status
#   'before_rebalance': {}, # the consul status before rebalance
#   'rebalance_action' : {'host':'x', 'old_role':'server', 'new_role':'agent', 'joins':''},
#   'after_rebalance': {} , # the consul status after rebalance
#   'action_started': '2018-11-10 12:00:00',
#   'action_finished': '2018-11-10 01:00:00',
#   'action_status': 'finished',
#   'last_updated':'2018-09-30 12:00:00'
#  }

consul_role_rebalance_table = Table(
    'consul_role_rebalance', meta,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('uuid', Text),
    Column('event_name', Text),
    Column('event_uuid', Text),
    Column('before_rebalance', Text),
    Column('rebalance_action', Text),
    Column('after_rebalance', Text),
    Column('action_started', DateTime),
    Column('action_finished', DateTime),
    Column('action_status', Text),
    Column('last_updated', DateTime),
    Column('last_error', Text)
)


def upgrade(migrate_engine):
    meta.bind = migrate_engine
    consul_role_rebalance_table.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    consul_role_rebalance_table.drop()
