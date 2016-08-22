from sqlalchemy import Table, Column, Boolean, Integer, String, DateTime, MetaData

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

node = Table(
    'nodes', meta,
    Column('id', Integer, primary_key=True),
    Column('deleted', Integer, default=None),
    Column('host', String(255), default=1),
    Column('member_type', Integer, default=None),
    Column('enabled', Boolean, default=False),
    Column('status', String(36), default=1),
    Column('updated_at', DateTime, default=None),
    Column('created_at', DateTime, default=None),
    Column('deleted_at', DateTime, default=None)
)


def upgrade(migrate_engine):
    meta.bind = migrate_engine
    cluster.create()
    node.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    node.drop()
    cluster.drop()

