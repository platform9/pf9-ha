# Copyright (c) 2018 Platform9 Systems Inc.
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
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Text

meta = MetaData()

# distributed architecture for host events processed by event process engine
# roles of different components
#  1. pf9-ha-slave : report host down/up event to pf9-hamgr
#  2. pf9-hamgr REST API : CRUD operations on availability zone, host up/down
#                          event processing status into/from database
#  3. pf9-hamgr engines  : scan requests/events from database , process
#                          unhandled requests/events, then update their status
# the event processing table should looks like:
#
# {
#  'id':'',             # primary key for indexing
#  'event_uuid':'',       # unique identifier of the event, foreign key to
#                         #  change_event table
#  'event_type':'',     # 'host_down' or 'host_up' event
#  'event_time':'',     # timestamp of the event
#  'host_name':'',        # host id which causes the event
#  'cluster_id':'',     # id of cluster that the above host in
#  'notification_uuid':'', # masakari notification id, foreign key to
#                          # masakari.notifications table
#  'notification_created':'', # timestamp when the masakari notification created
#  'notification_status':'', # current masakari notification status
#  'notification_updated':'' # timestamp of the last status is updated
# }

events_processing = Table(
        'events_processing', meta,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('event_uuid', Text, nullable=False),
        Column('event_type', Text),
        Column('event_time', DateTime),
        Column('host_name', Text),
        Column('cluster_id', Text),
        Column('notification_uuid', Text),
        Column('notification_created', DateTime),
        Column('notification_status', Text),
        Column('notification_updated', DateTime)
)

change_events_col_uuid = Column('uuid', Text)


def upgrade(migrate_engine):
    meta.bind = migrate_engine
    # create events_processing table
    events_processing.create()
    # modify the change_events table by adding uuid column
    change_events_col_uuid.create(Table('change_events', meta, autoload=True))

def downgrade(migrate_engine):
    meta.bind = migrate_engine
    # drop the events_processing table
    events_processing.drop()
    # drop the uuid column in change_events table
    change_events_col_uuid.drop(Table('change_events', meta, autoload=True))
