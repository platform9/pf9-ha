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

import logging

from contextlib import contextmanager

from hamgr import exceptions
from hamgr import states
from sqlalchemy import create_engine
from sqlalchemy import Column, Table, ForeignKey
from sqlalchemy import Boolean, DateTime, Integer, String, types
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError


from hamgr.exceptions import ClusterExists, ClusterNotFound, HostPartOfCluster

LOG = logging.getLogger(__name__)

Base = declarative_base()

_session_maker = None
_engine = None


class Cluster(Base):

    __tablename__ = 'clusters'
    __table_args__ = {'mysql_engine': 'InnoDB'}
    __mapper_args__ = {'always_refresh': True}

    id = Column(Integer, primary_key=True)
    deleted = Column(Integer, default=None)
    status = Column(String(36), default=1)
    enabled = Column(Boolean, default=False)
    updated_at = Column(DateTime, default=None)
    created_at = Column(DateTime, default=None)
    deleted_at = Column(DateTime, default=None)
    name = Column(String(255))
    task_state = Column(String(36), nullable=True)


def init(config, connection_string=None):
    conn_str = connection_string or config.get('database', 'sqlconnectURI')

    global _engine
    _engine = create_engine(conn_str)

    global _session_maker
    _session_maker = sessionmaker(bind=_engine, expire_on_commit=False)


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


def get_cluster(cluster_name_or_id, read_deleted=False):
    with dbsession() as session:
        clstr = _get_cluster(session, cluster_name_or_id,
                             read_deleted=read_deleted)
        if clstr is None:
            raise ClusterNotFound(cluster_name_or_id)
        return clstr


def _create_cluster(session, cluster_name, task_state):
    try:
        clstr = Cluster()
        clstr.name = cluster_name
        clstr.task_state = task_state
        session.add(clstr)
        return clstr
    except SQLAlchemyError as se:
        LOG.error('DB error: %s', se)
        raise


def create_cluster(cluster_name, task_state):
    with dbsession() as session:
        existing_cluster = _get_cluster(session, cluster_name)
        if existing_cluster is not None:
            raise ClusterExists(cluster_name)
        return _create_cluster(session, cluster_name, task_state)


def create_cluster_if_needed(cluster_name, task_state):
    with dbsession() as session:
        cluster = _get_cluster(session, cluster_name)
        if cluster is None:
            cluster = _create_cluster(session, cluster_name, task_state)
        return cluster


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
                # TODO: Check if the task state transition is valid. Till then
                #       log a warning.
                LOG.warn('Task state being updated from %s to %s', task_state,
                         state)
        if state not in states.VALID_TASK_STATES:
            raise exceptions.InvalidTaskState(state)
        db_cluster.task_state = state

