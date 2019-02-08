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
from sqlalchemy import MetaData
from sqlalchemy import Text
from sqlalchemy import Table

meta = MetaData()


def upgrade(migrate_engine):
    meta.bind = migrate_engine
    events_processing = Table('events_processing', meta, autoload=True)
    new_column = Column('error_state', Text, nullable=True)
    new_column.create(events_processing)


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    events_processing = Table('events_processing', meta, autoload=True)
    events_processing.c.error_state.drop()
