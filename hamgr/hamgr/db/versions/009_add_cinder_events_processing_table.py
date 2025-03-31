# Copyright (c) 2025 Platform9 Systems Inc.


from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, Text

meta = MetaData()

cinder_events_processing = Table(
    'cinder_events_processing', meta,
    Column('id', Integer, primary_key=True),
    Column('event_uuid', Text),
    Column('event_type', Text),
    Column('event_time', DateTime),
    Column('host_name', Text),
    Column('notification_status', Text),
    Column('notification_updated', DateTime),
    Column('error_state', Text),
    mysql_engine='InnoDB'
)


def upgrade(migrate_engine):
    meta.bind = migrate_engine
    cinder_events_processing.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    cinder_events_processing.drop()
