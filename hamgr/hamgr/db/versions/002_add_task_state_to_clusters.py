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
from sqlalchemy import String
from sqlalchemy import Table

meta = MetaData()


def upgrade(migrate_engine):
    meta.bind = migrate_engine
    clusters = Table('clusters', meta, autoload=True)
    new_column = Column('task_state', String(36), nullable=True)
    new_column.create(clusters)


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    clusters = Table('clusters', meta, autoload=True)
    clusters.c.task_state.drop()
