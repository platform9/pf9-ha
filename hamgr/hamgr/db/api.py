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

import datetime
from hamgr import exceptions
from hamgr import states
from hamgr import constants
from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import DateTime
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy import or_
from uuid import uuid4

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
    status = Column(String(36), default=None)
    enabled = Column(Boolean, default=False)
    updated_at = Column(DateTime, default=None)
    created_at = Column(DateTime, default=None)
    deleted_at = Column(DateTime, default=None)
    name = Column(String(255))
    task_state = Column(String(36), nullable=True)


class ChangeEvents(Base):
    __tablename__ = 'change_events'
    __table_args__ = {'mysql_engine': 'InnoDB'}
    __mapper_args__ = {'always_refresh': True}

    id = Column(Integer, primary_key=True)
    uuid = Column(Text)
    cluster = Column(Integer)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow())
    events = Column(Text, nullable=True)


class EventsProcessing(Base):
    __tablename__ = 'events_processing'
    __table_args__ = {'mysql_engine': 'InnoDB'}
    __mapper_args__ = {'always_refresh': True}

    id = Column(Integer, primary_key=True)
    event_uuid = Column(Text)
    event_type = Column(Text)
    event_time = Column(DateTime)
    host_name = Column(Text)
    cluster_id = Column(Text)
    notification_uuid = Column(Text)
    notification_created = Column(Text)
    notification_status = Column(Text)
    notification_updated = Column(DateTime)
    error_state = Column(Text)


class ConsulStatusInfo(Base):
    __tablename__ = 'consul_status'
    __table_args__ = {'mysql_engine': 'InnoDB'}
    __mapper_args__ = {'always_refresh': True}

    id = Column(Integer, primary_key=True)
    clusterId = Column('cluster_id', Integer)
    clusterName = Column('cluster_name', Text)
    leader = Column('leader', Text)
    peers = Column('peers', Text)
    members = Column('members', Text)
    kvstore = Column('kvstore', Text)
    joins = Column('joins', Text)
    lastUpdate = Column('last_updated', DateTime)
    lastEvent = Column('last_event', Text)


def init(config, connection_string=None):
    conn_str = connection_string or config.get('database', 'sqlconnectURI')

    global _engine
    _engine = create_engine(conn_str)

    global _session_maker
    _session_maker = sessionmaker(bind=_engine, expire_on_commit=False)


def _has_unsaved_changes(session):
    if any([session.dirty, session.new, session.deleted]):
        return True
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
    # when async operation of enable/disable, the 'enabled' is not
    # updated until 'status' changed to desired status, so need to only
    # return records where status is 'enabled'
    query = query.filter_by(enabled=True, status=constants.HA_STATE_ENABLED)
    clusters = query.all()
    return clusters


def get_all_active_clusters():
    with dbsession() as session:
        return _get_all_active_clusters(session)


def _get_cluster(session, cluster_name_or_id, read_deleted=False, ):
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
            raise exceptions.ClusterNotFound(cluster_name_or_id)
        return clstr

def get_all_unhandled_enable_or_disable_requests():
    with dbsession() as session:
        try:
            query = session.query(Cluster)
            # unhandled requests should be marked as 'request-enable' or
            # 'request-disable'
            query = query.filter(or_(
                Cluster.status == constants.HA_STATE_REQUEST_ENABLE,
                Cluster.status == constants.HA_STATE_REQUEST_DISABLE))
            return query.all()
        except SQLAlchemyError as se:
            LOG.error('DB error when query unhandled cluster requests : %s', se)


def update_request_status(cluster_id, status):
    if not cluster_id:
        raise ArgumentException('cluster_id is null or empty')
    if not status or status not in constants.HA_STATE_ALL:
        raise ArgumentException('status is null or empty or invalid')
    with dbsession() as session:
        db_cluster = _get_cluster(session, cluster_id)
        db_cluster.status = status

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
            raise exceptions.ClusterExists(cluster_name)
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


def create_change_event(cluster_id, events, event_id= ''):
    if cluster_id is None:
        raise ArgumentException("cluster_id is null or empty")
    if events is None:
        raise ArgumentException("events argument is null or empty")
    with dbsession() as session:
        try:
            change = ChangeEvents()
            change.uuid = str(uuid4()) if not event_id else event_id
            change.cluster = int(cluster_id)
            change.timestamp = datetime.datetime.utcnow()
            change.events = str(events)
            session.add(change)
            LOG.debug('successfully committed change event : %s', str(change))
            return change
        except SQLAlchemyError as se:
            LOG.error('DB error when create change event : %s', se)


# get change envents between given time range for cluster id, host name
# and event type
def get_change_events_between_times(cluster_id,
                                    host_name,
                                    event_type,
                                    start_time,
                                    end_time):
    if cluster_id is None:
        raise ArgumentException('cluster_id is null or empty')
    if host_name is None:
        raise ArgumentException('host_name is null or empty')
    if start_time > end_time:
        raise ArgumentException('start_time is bigger than end_time')
    if event_type not in constants.VALID_EVENT_TYPES:
        raise ArgumentException('event_type is null or empty or not valid')
    etype = ''
    if event_type == constants.EVENT_HOST_DOWN:
        etype = '\'eventType\': 2'
    elif event_type == constants.EVENT_HOST_UP:
        etype = '\'eventType\': 1'
    with dbsession() as session:
        try:
            query = session.query(ChangeEvents)
            query = query.filter(ChangeEvents.cluster == cluster_id)
            query = query.filter(
                ChangeEvents.events.contains(host_name),
                ChangeEvents.events.contains(etype))
            query = query.filter(
                ChangeEvents.timestamp - start_time >= 0,
                ChangeEvents.timestamp - end_time <= 0
            )
            return query.all()
        except SQLAlchemyError as se:
            LOG.error('DB error when query change events : %s', se)


# create event, return event created to caller
def create_processing_event(event_uuid, event_type, host_name, cluster_id):
    if event_type not in constants.VALID_EVENT_TYPES:
        raise ArgumentException('event_type not in %s' % \
                                str(constants.VALID_EVENT_TYPES))
    if host_name is None:
        raise ArgumentException('host_name is empty')
    if cluster_id is None:
        raise ArgumentException('cluster_id is empty')
    with dbsession() as session:
        try:
            ep = EventsProcessing()
            ep.event_uuid = event_uuid
            ep.event_type = event_type
            ep.event_time = datetime.datetime.utcnow()
            ep.host_name = str(host_name)
            ep.cluster_id = str(cluster_id)
            session.add(ep)
            return ep
        except Exception:
            LOG.error('failed to create event', exc_info=True)
        return None


# get all unhandled events
def get_all_unhandled_processing_events():
    with dbsession() as session:
        try:
            query = session.query(EventsProcessing)
            query = query.filter(or_(
                EventsProcessing.notification_status == None,
                ~EventsProcessing.notification_status.in_(
                    constants.HANDLED_STATES)))
            records = query.all()
            return records
        except Exception as e:
            LOG.error('failed to get all unhandled processing events : %s ',
                      str(e))
        return None


# query event by id
def get_processing_event_by_id(event_uuid):
    if event_uuid is None:
        raise ArgumentException('event_uuid is empty')
    with dbsession() as session:
        try:
            query = session.query(EventsProcessing)
            query = query.filter_by(event_uuid=event_uuid)
            ep = query.first()
            return ep
        except Exception:
            LOG.error('failed to get event %s', str(event_uuid), exc_info=True)
        return None


# get processing events with given time range
def get_processing_events_between_times(event_type,
                                        host_name,
                                        cluster_id,
                                        start_time,
                                        end_time):
    if not event_type or event_type not in constants.VALID_EVENT_TYPES:
        raise ArgumentException('event_type is null or empty or invalid')
    if host_name is None:
        raise ArgumentException('host_name is empty')
    with dbsession() as session:
        try:
            query = session.query(EventsProcessing)
            query = query.filter(
                EventsProcessing.event_type == event_type,
                EventsProcessing.host_name == host_name,
                EventsProcessing.cluster_id == cluster_id)
            query = query.filter(EventsProcessing.event_time - start_time >= 0,
                                 EventsProcessing.event_time - end_time <= 0)
            return query.all()
        except Exception:
            LOG.error('failed to query events for %s', str(host_name),
                      exc_info=True)
        return None


# update event for notification fields
def update_processing_event_with_notification(event_uuid, notification_uuid,
                                              notification_created,
                                              notification_status,
                                              error_state = ''):
    if event_uuid is None:
        raise ArgumentException('event_uuid is empty')
    with dbsession() as session:
        try:
            query = session.query(EventsProcessing)
            query = query.filter_by(event_uuid=event_uuid)
            ep = query.first()
            if not ep:
                LOG.info('event %s does not exist', event_uuid)
                return
            LOG.debug('found processing event %s : %s', event_uuid, str(ep))
            ep.notification_uuid = notification_uuid
            ep.notification_created = notification_created
            ep.notification_status = notification_status
            ep.error_state = error_state
            ep.notification_updated = datetime.datetime.utcnow()
            return ep
        except Exception:
            LOG.error('failed to update event with notification', exc_info=True)
        return None


def get_latest_consul_status(aggregate_id=None):
    with dbsession() as session:
        try:
            records = []
            query = session.query(ConsulStatusInfo)
            if aggregate_id is not None:
                # the passed in aggregate_id maps to clusterName in hamgr db
                query = query.filter_by(clusterName=str(aggregate_id))
                query = query.order_by(ConsulStatusInfo.lastUpdate.desc())
                records.append(query.first())
            else:
                # find distinct clusters, then returns most recent one
                # from each group
                ids = []
                for e in query.distinct(ConsulStatusInfo.clusterName).all():
                    if e is not None:
                        ids.append(e.clusterName)
                for id in ids:
                    query = session.query(ConsulStatusInfo)
                    query = query.filter_by(clusterName=str(id))
                    query = query.order_by(ConsulStatusInfo.lastUpdate.desc())
                    record = query.first()
                    records.append(record)
            return records
        except Exception:
            LOG.error('failed to get most recent consul status record',
                      exc_info=True)
        return []


def add_consul_status(cluster_id,
                      cluster_name,
                      leader,
                      peers,
                      members,
                      kv='',
                      joins='',
                      last_event=''):
    if leader is None:
        raise ArgumentException("leader is null or empty")
    if peers is None:
        raise ArgumentException("peers argument is null or empty")
    if members is None:
        raise ArgumentException("members argument is null or empty")
    with dbsession() as session:
        try:
            status = ConsulStatusInfo()
            status.clusterId = cluster_id
            status.clusterName = cluster_name
            status.leader = leader
            status.peers = peers
            status.members = members
            status.kvstore = kv
            status.joins = joins
            status.lastUpdate = datetime.datetime.utcnow()
            status.lastEvent = str(last_event)
            session.add(status)
            LOG.debug('successfully committed consul status : %s', str(status))
            return status
        except SQLAlchemyError as se:
            LOG.error('DB error when create consul status : %s', se)
