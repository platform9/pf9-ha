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


from sqlalchemy import Table, Column, Boolean, Integer, String, DateTime, MetaData, ForeignKey

meta = MetaData()

cluster = Table(
    'clusters', meta,
    Column('id', Integer, primary_key=True),
    Column('deleted', Integer, default=None),
    Column('name', String(255), default=None),
    Column('enabled', Boolean, default=False),
    Column('status', String(36), default=1),
    Column('updated_at', DateTime, default=None),
    Column('created_at', DateTime, default=None),
    Column('deleted_at', DateTime, default=None)
)

def upgrade(migrate_engine):
    meta.bind = migrate_engine
    cluster.create()



def downgrade(migrate_engine):
    meta.bind = migrate_engine
    cluster.drop()

