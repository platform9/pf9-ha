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

# the status of consul cluster
#  {
#   'cluster_id':1,
#   'leader':'10.97.0.9:8300',
#   'peers' : ["10.97.0.46:8300", "10.97.0.31:8300", "10.97.0.9:8300"]
#   'members' : [],
#   'kv':[]
#   'joins':'',
#   'last_updated':'2018-09-30 12:00:00',
#   'last_event': ''
#  }

consul_status_table = Table(
    'consul_status', meta,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('cluster_id', Integer),
    Column('cluster_name', Text),
    Column('leader', Text),
    Column('peers', Text),
    Column('members', Text),
    Column('kvstore', Text),
    Column('joins', Text),
    Column('last_updated', DateTime),
    Column('last_event', Text)
)


def upgrade(migrate_engine):
    meta.bind = migrate_engine
    consul_status_table.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    consul_status_table.drop()
