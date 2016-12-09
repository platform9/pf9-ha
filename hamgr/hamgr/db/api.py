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

from contextlib import contextmanager
from enum import Enum
from hamgr import exceptions
from hamgr import states
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
    task_state = Column(String(36), nullable=True)


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

    # None == not a member, 0 == leader, 1 == server, 2 == agent
    member_type = Column(Integer, default=None)
    cluster_id = Column(Integer, ForeignKey('clusters.id'), nullable=True)


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


def _get_all_active_clusters(session):
    query = session.query(Cluster)
    clusters = query.all()
    return [cluster for cluster in clusters if cluster.enabled == True]


def get_all_active_clusters():
    with dbsession() as session:
        return _get_all_active_clusters(session)


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


def _disable_all_nodes_in_cluster(session, cluster_id):
    rcount = session.query(Node).filter_by(
            cluster_id=cluster_id).update({"cluster_id": None},
                                           synchronize_session=False)
    # Without the following commit statement cluster_id does not get updated to
    # NULL
    session.commit()
    LOG.debug('Updated %(count)s rows', {'count': str(rcount)})


def disable_all_nodes_in_cluster(cluster_id):
    with dbsession() as session:
        return _disable_all_nodes_in_cluster(session, cluster_id)


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
        return clstr.id
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
        cluster_id = cluster.id
        if cluster is None:
            cluster_id = _create_cluster(session, cluster_name)
        return cluster_id


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

def update_cluster_task_state(cluster_id, state):
    with dbsession() as session:
        db_cluster = _get_cluster(session, cluster_id)
        task_state = db_cluster.task_state
        if task_state and state is not None:
            if task_state == state:
                # NOOP
                LOG.debug('Updating task_state with same value - {val}'.format(
                    val=state))
            else:
                raise exceptions.UpdateConflict(cluster_id, task_state, state)
        if state not in states.VALID_TASK_STATES:
            raise exceptions.InvalidTaskState(state)
        db_cluster.task_state = state

def get_cluster_task_state(cluster_id):
    with dbsession() as session:
        db_cluster = _get_cluster(session, cluster_id)
        return db_cluster.task_state

