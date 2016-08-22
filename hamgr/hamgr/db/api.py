#
# Copyright (C) 2016, Platform9 Systems. All Rights Reserved.
#
from enum import Enum
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy import Column, Table, ForeignKey
from sqlalchemy import Boolean, DateTime, Integer, String, types
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

import logging
from hamgr.exceptions import ClusterExists, ClusterNotFound, HostPartOfCluster

LOG = logging.getLogger(__name__)

Base = declarative_base()

_session_maker = None


class BaseTable(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    __table_args__ = {'mysql_engine': 'InnoDB'}
    __mapper_args__ = {'always_refresh': True}

    id = Column(Integer, primary_key=True)
    deleted = Column(Integer, default=None)
    status = Column(String(36), default=1)
    enabled = Column(Boolean, default=False)
    updated_at = Column(DateTime, default=None)
    created_at = Column(DateTime, default=None)
    deleted_at = Column(DateTime, default=None)


class Cluster(BaseTable, Base):
    __tablename__ = 'clusters'
    name = Column(String(255))


class Role(Enum):
    """
    Numerical equivalent of DB role field.
    """
    leader = 0
    server = 1
    agent = 2


class Node(BaseTable, Base):
    __tablename__ = 'nodes'
    host = Column(String(255))
    allocated = Column(Boolean, default=False)

    # None == not a member, 0 == leader, 1 == server, 2 == agent
    member_type = Column(Integer, default=None)
    cluster_id = Column(Integer, ForeignKey('clusters.id'))


def init(config):
    conn_str = config.get('database', 'sqlconnectURI')
    engine_handle = create_engine(conn_str)

    global _session_maker
    _session_maker = sessionmaker(bind=engine_handle)


def _has_unsaved_changes(session):
    if any([session.dirty, session.new, session.deleted]):
        return True
    else:
        return False

@contextmanager
def dbsession():
    global _session_maker
    db_session = _session_maker()
    try:
        yield db_session
        if _has_unsaved_changes(db_session):
            db_session.commit()
    except SQLAlchemyError as se:
        LOG.error('Error working with db sesssion: %s', se)
        if _has_unsaved_changes(db_session):
            db_session.rollback()
    finally:
        db_session.close()


def _get_all_clusters(session, read_deleted=False):
    query = session.query(Cluster)
    if read_deleted is False:
        query = query.filter_by(deleted=None)
    return query.all()


def _get_cluster(session, cluster_name_or_id, read_deleted=False,):
    query = session.query(Cluster)
    if read_deleted is False:
        query = query.filter_by(deleted=None)
    if isinstance(cluster_name_or_id, basestring):
        query = query.filter_by(name=cluster_name_or_id)
    else:
        query = query.filter_by(id=cluster_name_or_id)
    return query.first()


def get_all_clusters(read_deleted=False):
    with dbsession() as session:
        return _get_all_clusters(session, read_deleted=read_deleted)


def _get_all_nodes(session, read_deleted=False, cluster_id=None, host=None, member_type=None):
    query = session.query(Node)
    if read_deleted is False:
        query = query.filter_by(deleted=None)
    if cluster_id is not None:
        query = query.filter_by(cluster_id=cluster_id)
    if host is not None:
        query = query.filter_by(host=host)
    if member_type is not None:
        query = query.filter_by(member_type=member_type)
    return query.all()


def get_all_nodes(read_deleted=False, cluster_id=None, member_type=None):
    with dbsession() as session:
        return _get_all_nodes(session, read_deleted=read_deleted,
                              cluster_id=cluster_id, member_type=member_type)


def get_cluster(cluster_name_or_id, read_deleted=False):
    with dbsession() as session:
        clstr = _get_cluster(session, cluster_name_or_id,
                             read_deleted=read_deleted)
        if clstr is None:
            raise ClusterNotFound(cluster_name_or_id)
        return clstr


def _create_cluster(session, cluster_name):
    try:
        clstr = Cluster()
        clstr.name = cluster_name
        session.add(clstr)
    except SQLAlchemyError as se:
        LOG.error('DB error: %s', se)
        raise


def create_cluster(cluster_name):
    with dbsession() as session:
        existing_cluster = _get_cluster(session, cluster_name)
        if existing_cluster is not None:
            raise ClusterExists(cluster_name)


def create_cluster_if_needed(cluster_name):
    with dbsession() as session:
        cluster = _get_cluster(session, cluster_name)
        if cluster is None:
            _create_cluster(session, cluster_name)


def add_nodes_if_needed(hosts, cluster_id):
    with dbsession() as session:
        nodes = _get_all_nodes(session)
        lookup_hosts = dict((n.host, n) for n in nodes)
        for h in hosts:
            if h not in lookup_hosts:
                db_host = Node()
                db_host.host = h
                db_host.cluster_id = cluster_id
                session.add(db_host)
            else:
                db_host = lookup_hosts[h]
                if db_host.cluster_id != cluster_id:
                    db_host.cluster_id = cluster_id


def _get_node(session, node_name, read_deleted=False):
    query = session.query(Node)
    if read_deleted is False:
        query = query.filter_by(deleted=None)
    return query.filter_by(host=node_name).one()


def update_node(node_name, cluster_id, role):
    with dbsession() as session:
        db_node = _get_node(session, node_name)
        db_node.cluster_id = cluster_id
        db_node.role = role


def update_cluster(cluster_id, enabled):
    with dbsession() as session:
        db_cluster = _get_cluster(session, cluster_id)
        db_cluster.enabled = enabled







