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

import datetime

from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Text

meta = MetaData()

change_events = Table(
        'change_events', meta,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('cluster', Integer, default=None),
        Column('timestamp', DateTime, default=datetime.datetime.utcnow()),
        Column('events', Text)
)


def upgrade(migrate_engine):
    meta.bind = migrate_engine
    change_events.create()

def downgrade(migrate_engine):
    meta.bind = migrate_engine
    change_events.drop()
