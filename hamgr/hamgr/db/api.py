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

import logging

from contextlib import contextmanager

import datetime
from shared.exceptions import ha_exceptions as exceptions
from shared import constants
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
from sqlalchemy import LargeBinary
from sqlalchemy import or_
from uuid import uuid4
from shared.constants import LOGGER_PREFIX

LOG = logging.getLogger(LOGGER_PREFIX + __name__)

Base = declarative_base()

_session_maker = None
_engine = None
_msession_maker = None
_mengine = None

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
    events = Column(LargeBinary, nullable=True)


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
    peers = Column('peers', LargeBinary)
    members = Column('members', LargeBinary)
    kvstore = Column('kvstore', LargeBinary)
    joins = Column('joins', Text)
    lastUpdate = Column('last_updated', DateTime)
    lastEvent = Column('last_event', Text)


class ConsulRoleRebalanceRecord(Base):
    __tablename__ = 'consul_role_rebalance'
    __table_args__ = {'mysql_engine': 'InnoDB'}
    __mapper_args__ = {'always_refresh': True}

    id = Column(Integer, primary_key=True)
    uuid = Column('uuid', Text)
    event_name = Column('event_name', Text)
    event_uuid = Column('event_uuid', Text)
    before_rebalance = Column('before_rebalance', LargeBinary)
    rebalance_action = Column('rebalance_action', Text)
    after_rebalance = Column('after_rebalance', LargeBinary)
    action_started = Column('action_started', DateTime)
    action_finished = Column('action_finished', DateTime)
    action_status = Column('action_status', Text)
    last_updated = Column('last_updated', DateTime)
    last_error = Column('last_error', Text)


class CinderEventsProcessing(Base):
    __tablename__ = 'cinder_events_processing'
    __table_args__ = {'mysql_engine': 'InnoDB'}
    __mapper_args__ = {'always_refresh': True}

    id = Column(Integer, primary_key=True)
    event_uuid = Column(Text)
    event_type = Column(Text)
    event_time = Column(DateTime)
    host_name = Column(Text)
    notification_status = Column(Text)
    notification_updated = Column(DateTime)
    error_state = Column(Text)


class Hosts(Base):
    __tablename__ = 'hosts'
    __table_args__ = {'mysql_engine': 'InnoDB'}
    __mapper_args__ = {'always_refresh': True}

    id = Column(Integer, primary_key=True)
    deleted = Column(Integer, default=None)
    updated_at = Column(DateTime, default=None)
    created_at = Column(DateTime, default=None)
    deleted_at = Column(DateTime, default=None)
    name = Column(String(255))
    type = Column(String(255))
    uuid = Column(String(36))
    failover_segment_id = Column(String(36))
    reserved = Column(Integer)
    on_maintenance = Column(Integer)
    control_attributes = Column(Text)

def init(config, connection_string=None):
    conn_str = connection_string or config.get('database', 'sqlconnectURI')

    global _engine
    _engine = create_engine(conn_str)

    global _session_maker
    _session_maker = sessionmaker(bind=_engine, expire_on_commit=False)


def connect_masakari(config, connection_string=None):
    mconn_str = connection_string or config.get('masakari', 'sqlconnectURI')

    global _mengine
    _mengine = create_engine(mconn_str)

    global _msession_maker
    _msession_maker = sessionmaker(bind=_mengine, expire_on_commit=False)

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
        LOG.error('Error working with db session: %s', se)
        if _has_unsaved_changes(db_session):
            db_session.rollback()
    finally:
        db_session.close()

@contextmanager
def mdbsession():
    global _msession_maker
    mdb_session = _msession_maker()
    try:
        yield mdb_session
        if _has_unsaved_changes(mdb_session):
            mdb_session.commit()
    except SQLAlchemyError as se:
        LOG.error('Error working with db session: %s', se)
        if _has_unsaved_changes(mdb_session):
            mdb_session.rollback()
    finally:
        mdb_session.close()

def _get_all_clusters(session, read_deleted=False):
    query = session.query(Cluster)
    if read_deleted is False:
        query = query.filter_by(deleted=0)
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


def _get_cluster(session, cluster_name_or_id, read_deleted=False):
    query = session.query(Cluster)
    # since id or name are unique, no matter it is deleted,
    # when filter by name or id, the record will always be returned
    if read_deleted:
        query = query.filter(Cluster.deleted != 0)

    if isinstance(cluster_name_or_id, str):
        query = query.filter_by(name=cluster_name_or_id)
    else:
        query = query.filter_by(id=cluster_name_or_id)

    return query.first()


def get_all_clusters(read_deleted=False):
    with dbsession() as session:
        return _get_all_clusters(session, read_deleted=read_deleted)


def get_cluster(cluster_name_or_id, read_deleted=False, raise_exception=False):
    with dbsession() as session:
        clstr = _get_cluster(session, cluster_name_or_id,
                             read_deleted=read_deleted)
        if clstr is None and raise_exception:
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
        raise exceptions.ArgumentException('cluster_id is null or empty')
    if not status or status not in constants.HA_STATE_ALL:
        raise exceptions.ArgumentException('status is null or empty or invalid')
    with dbsession() as session:
        db_cluster = _get_cluster(session, cluster_id)
        if not db_cluster:
            LOG.debug('update_request_status: no cluster found with id %s', str(cluster_id))
            return
        db_cluster.status = status
        db_cluster.updated_at = datetime.datetime.utcnow()


def _create_cluster(session, cluster_name, task_state):
    try:
        clstr = Cluster()
        clstr.name = cluster_name
        clstr.task_state = task_state
        clstr.deleted = 0
        clstr.updated_at = datetime.datetime.utcnow()
        clstr.created_at = datetime.datetime.utcnow()
        session.add(clstr)
        return clstr
    except SQLAlchemyError as se:
        LOG.error('DB error when creating cluster %s: %s', cluster_name, se)
        raise


def create_cluster(cluster_name, task_state):
    with dbsession() as session:
        existing_cluster = _get_cluster(session, cluster_name)
        if existing_cluster is not None:
            raise exceptions.ClusterExists(cluster_name)
        return _create_cluster(session, cluster_name, task_state)

def is_cluster_exist(cluster_name):
    with dbsession() as session:
        existing = _get_cluster(session, str(cluster_name))
        if existing is None:
            return False
        return True

def create_cluster_if_needed(cluster_name, task_state):
    with dbsession() as session:
        cluster = _get_cluster(session, cluster_name)
        if cluster is None:
            cluster = _create_cluster(session, cluster_name, task_state)
        return cluster


def update_cluster(cluster_id, enabled):
    with dbsession() as session:
        db_cluster = _get_cluster(session, cluster_id)
        if not db_cluster:
            LOG.debug('update_cluster: no cluster found with id %s for action %s', str(cluster_id), str(enabled))
            return
        db_cluster.updated_at = datetime.datetime.utcnow()
        db_cluster.enabled = enabled
        if not enabled:
            db_cluster.deleted = db_cluster.id
            db_cluster.deleted_at = datetime.datetime.utcnow()
        else:
            db_cluster.deleted = 0
            db_cluster.deleted_at = None


def update_cluster_task_state(cluster_id, state):
    with dbsession() as session:
        db_cluster = _get_cluster(session, cluster_id)
        if not db_cluster:
            LOG.debug('update_cluster_task_state: no cluster found with id %s for new task state %s',
                     str(cluster_id), str(state))
            return
        task_state = db_cluster.task_state
        if task_state and state is not None:
            if task_state == state:
                # NOOP
                LOG.debug('Updating task_state with same value - {val}'.format(
                    val=state))
            else:
                # TODO: Check if the task state transition is valid. Till then
                #       log a warning.
                LOG.warning('Task state being updated from %s to %s', task_state,
                         state)
        if state not in constants.VALID_TASK_STATES:
            raise exceptions.InvalidTaskState(state)
        db_cluster.task_state = state
        db_cluster.updated_at = datetime.datetime.utcnow()


def create_change_event(cluster_id, events, event_id=''):
    if cluster_id is None:
        raise exceptions.ArgumentException("cluster_id is null or empty")
    if events is None:
        raise exceptions.ArgumentException("events argument is null or empty")
    with dbsession() as session:
        try:
            change = ChangeEvents()
            change.uuid = str(uuid4()) if not event_id else event_id
            change.cluster = int(cluster_id)
            change.timestamp = datetime.datetime.utcnow()
            change.events = str(events).encode()
            session.add(change)
            LOG.debug('successfully committed change event : %s', str(change))
            return change
        except SQLAlchemyError as se:
            LOG.error('DB error when creating change event : %s', se)


def get_hosts_by_name(name):
    if name is None:
        raise exceptions.ArgumentException('host name is empty')
    with mdbsession() as session:
        try:
            query = session.query(Hosts)
            query = query.filter_by(name=name, deleted=0)
            result = query.first()
            return result
        except Exception:
            LOG.error('failed to get the host in masakari DB  %s', str(name), exc_info=True)
    return None

def get_change_event_by_id(uuid):
    if uuid is None:
        raise exceptions.ArgumentException('uuid is empty')
    with dbsession() as session:
        try:
            query = session.query(ChangeEvents)
            query = query.filter_by(uuid=uuid)
            ep = query.first()
            return ep
        except Exception:
            LOG.error('failed to get change event %s', str(uuid), exc_info=True)
    return None

# get change envents between given time range for cluster id, host name
# and event type
def get_change_events_between_times(cluster_id,
                                    host_name,
                                    event_type,
                                    start_time,
                                    end_time):
    if cluster_id is None:
        raise exceptions.ArgumentException('cluster_id is null or empty')
    if host_name is None:
        raise exceptions.ArgumentException('host_name is null or empty')
    if start_time > end_time:
        raise exceptions.ArgumentException('start_time is bigger than end_time')
    if event_type not in constants.VALID_EVENT_TYPES:
        raise exceptions.ArgumentException('event_type is null or empty or not valid')
    if event_type == constants.EVENT_HOST_DOWN:
        etype = '\'eventType\': 2'
    elif event_type == constants.EVENT_HOST_UP:
        etype = '\'eventType\': 1'
    with dbsession() as session:
        try:
            query = session.query(ChangeEvents)
            query = query.filter(ChangeEvents.cluster == cluster_id)
            query = query.filter(
                ChangeEvents.events.like('%{0}%'.format(host_name).encode()),
                ChangeEvents.events.like('%{0}%'.format(etype).encode()))
            query = query.filter(
                ChangeEvents.timestamp >= start_time,
                ChangeEvents.timestamp <= end_time
            )
            #LOG.debug('SQL query: %s', str(query.statement.compile(compile_kwargs={"literal_binds": True})))
            return query.all()
        except SQLAlchemyError as se:
            LOG.error('DB error when query change events : %s', se)


# create event, return event created to caller
def create_processing_event(event_uuid, event_type, host_name, cluster_id, notification_status = '', error_state=''):
    if event_type not in constants.VALID_EVENT_TYPES:
        raise exceptions.ArgumentException('event_type not in %s' %
                                           str(constants.VALID_EVENT_TYPES))
    if host_name is None:
        raise exceptions.ArgumentException('host_name is empty')
    if cluster_id is None:
        raise exceptions.ArgumentException('cluster_id is empty')
    with dbsession() as session:
        try:
            ep = EventsProcessing()
            ep.event_uuid = event_uuid
            ep.event_type = event_type
            ep.event_time = datetime.datetime.utcnow()
            ep.host_name = str(host_name)
            ep.cluster_id = str(cluster_id)
            ep.notification_status = notification_status
            ep.error_state = error_state
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
        raise exceptions.ArgumentException('event_uuid is empty')
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
                                        end_time,
                                        unhandled_only=True):
    if not event_type or event_type not in constants.VALID_EVENT_TYPES:
        raise exceptions.ArgumentException('event_type is null or empty or invalid')
    if host_name is None:
        raise exceptions.ArgumentException('host_name is empty')
    with dbsession() as session:
        try:
            query = session.query(EventsProcessing)
            query = query.filter(
                EventsProcessing.event_type == event_type,
                EventsProcessing.host_name == host_name,
                EventsProcessing.cluster_id == cluster_id)
            query = query.filter(EventsProcessing.event_time >= start_time,
                                 EventsProcessing.event_time <= end_time)
            if unhandled_only:
                query = query.filter(~EventsProcessing.notification_status.in_(constants.HANDLED_STATES))
            return query.all()
        except Exception:
            LOG.error('failed to query events for %s', str(host_name),
                      exc_info=True)
        return None


# update event for notification fields
def update_processing_event_with_notification(event_uuid, notification_uuid,
                                              notification_created,
                                              notification_status,
                                              error_state='',
                                              update_all=False):
    if event_uuid is None:
        raise exceptions.ArgumentException('event_uuid is empty')
    with dbsession() as session:
        try:
            query = session.query(EventsProcessing)
            query = query.filter_by(event_uuid=event_uuid)
            if update_all:
                result = query.all()
            else:
                result = [query.first()]
            if not result:
                LOG.warning('update ignored: event %s does not exist', event_uuid)
                return
            for ep in result:
                LOG.debug('found processing event %s : %s', event_uuid, str(ep))
                ep.notification_uuid = notification_uuid
                ep.notification_created = notification_created
                ep.notification_status = notification_status
                ep.error_state = error_state
                ep.notification_updated = datetime.datetime.utcnow()
            return result
        except Exception:
            LOG.error('failed to update event with notification', exc_info=True)
        return None


def get_latest_consul_status(availability_zone=None):
    with dbsession() as session:
        try:
            records = []
            query = session.query(ConsulStatusInfo)
            if availability_zone is not None:
                # the passed in availability_zone maps to clusterName in hamgr db
                query = query.filter_by(clusterName=availability_zone)
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
        raise exceptions.ArgumentException("leader is null or empty")
    if peers is None:
        raise exceptions.ArgumentException("peers argument is null or empty")
    if members is None:
        raise exceptions.ArgumentException("members argument is null or empty")
    with dbsession() as session:
        try:
            status = ConsulStatusInfo()
            status.clusterId = cluster_id
            status.clusterName = cluster_name
            status.leader = leader
            status.peers = peers.encode()
            status.members = members.encode()
            status.kvstore = kv.encode()
            status.joins = joins
            status.lastUpdate = datetime.datetime.utcnow()
            status.lastEvent = str(last_event)
            session.add(status)
            LOG.debug('successfully committed consul status : %s', str(status))
            return status
        except SQLAlchemyError as se:
            LOG.error('DB error when create consul status : %s', se)
            raise


def add_consul_role_rebalance_record(event_name,
                                     event_uuid,
                                     before_rebalance,
                                     rebalance_action,
                                     request_uuid):
    with dbsession() as session:
        try:
            record = ConsulRoleRebalanceRecord()
            record.uuid = str(request_uuid)
            record.event_name = event_name
            record.event_uuid = str(event_uuid)
            record.before_rebalance = str(before_rebalance).encode()
            record.rebalance_action = str(rebalance_action)
            record.last_updated = datetime.datetime.utcnow()
            session.add(record)
            LOG.debug('successfully commited consul role rebalance record : %s', str(record))
            return record.uuid
        except SQLAlchemyError as se:
            LOG.error('DB error when create consul role rebalance record : %s', se)
        return None


def get_consul_role_balance_record_by_uuid(uuid):
    with dbsession() as session:
        try:
            query = session.query(ConsulRoleRebalanceRecord)
            query = query.filter_by(uuid=uuid)
            record = query.first()
            return record
        except SQLAlchemyError as se:
            LOG.error('DB error when getting consul role rebalance record for uuid %s : %s', uuid, se)
        return None


def get_unhandled_consul_role_rebalance_records_by_action(action):
    with dbsession() as session:
        try:
            query = session.query(ConsulRoleRebalanceRecord)
            query = query.filter_by(rebalance_action=str(action))
            query = query.filter_by(action_status=None)
            records = query.all()
            return records
        except SQLAlchemyError as se:
            LOG.error('DB error when getting consul role rebalance records for action %s : %s', str(action), se)
        return None


def get_consul_role_balance_records_for_event(event_uuid):
    with dbsession() as session:
        try:
            query = session.query(ConsulRoleRebalanceRecord)
            query = query.filter_by(event_uuid=event_uuid)
            records = query.all()
            return records
        except SQLAlchemyError as se:
            LOG.error('DB error when getting consul role rebalance records for event  %s : %s', event_uuid, se)
        return None


def get_all_unhandled_consul_role_rebalance_requests():
    with dbsession() as session:
        try:
            query = session.query(ConsulRoleRebalanceRecord)
            # only return records whose status are not set, ignore 'running', 'aborted', 'finished' records
            query = query.filter_by(action_status=None)
            records = query.all()
            return records
        except SQLAlchemyError as se:
            LOG.error('DB error when getting unhandled consul role rebalance requests : %s', se)
        return None


def update_consul_role_rebalance(uuid,
                                 after_rebalance,
                                 action_finished,
                                 action_status,
                                 last_error):
    with dbsession() as session:
        try:
            query = session.query(ConsulRoleRebalanceRecord)
            query = query.filter_by(uuid=uuid)
            record = query.first()
            if not record:
                LOG.warning('no consul role rebalance record with uuid %s found', uuid)
                return None
            if after_rebalance:
                record.after_rebalance = str(after_rebalance).encode()
            if action_finished:
                record.action_finished = action_finished
            else:
                record.action_finished = datetime.datetime.utcnow()
            if action_status:
                record.action_status = action_status
            if last_error:
                record.last_error = last_error
            record.last_updated = datetime.datetime.utcnow()
            return record
        except SQLAlchemyError as se:
            LOG.error('DB error when getting consul role rebalance record  %s : %s', uuid, se)
        return None


# create cinder processing event, return event created to caller
def create_cinder_processing_event(event_uuid, event_type, host_name, notification_status='', error_state=''):
    """Create a new cinder processing event.

    :param event_uuid: UUID of the event
    :param event_type: Type of the event (host-up or host-down)
    :param host_name: Name of the host
    :param notification_status: Status of the notification
    :param error_state: Error state if any
    :return: Created event object
    """
    with dbsession() as session:
        event = CinderEventsProcessing()
        event.event_uuid = event_uuid
        event.event_type = event_type
        event.event_time = datetime.datetime.utcnow()
        event.host_name = host_name
        event.notification_status = notification_status
        event.error_state = error_state
        session.add(event)
        session.commit()
        return event


# get all unhandled cinder events
def get_all_unhandled_cinder_processing_events():
    """Get all unhandled cinder processing events.

    :return: List of unhandled cinder events
    """
    with dbsession() as session:
        query = session.query(CinderEventsProcessing)
        query = query.filter(or_(
            CinderEventsProcessing.notification_status == '',
            CinderEventsProcessing.notification_status == constants.STATE_NEW,
            CinderEventsProcessing.notification_status == constants.STATE_RUNNING
        ))
        return query.all()


# query cinder event by id
def get_cinder_processing_event_by_id(event_uuid):
    """Get cinder processing event by UUID.

    :param event_uuid: UUID of the event
    :return: Event object if found, None otherwise
    """
    with dbsession() as session:
        query = session.query(CinderEventsProcessing)
        query = query.filter(CinderEventsProcessing.event_uuid == event_uuid)
        return query.first()


# get cinder processing events with given time range
def get_cinder_processing_events_between_times(event_type,
                                        host_name,
                                        start_time,
                                        end_time,
                                        unhandled_only=True):
    """Get cinder processing events between given time range.

    :param event_type: Type of the event
    :param host_name: Name of the host
    :param start_time: Start time
    :param end_time: End time
    :param unhandled_only: Whether to get only unhandled events
    :return: List of events
    """
    with dbsession() as session:
        query = session.query(CinderEventsProcessing)
        query = query.filter(CinderEventsProcessing.event_type == event_type)
        query = query.filter(CinderEventsProcessing.host_name == host_name)
        query = query.filter(CinderEventsProcessing.event_time >= start_time)
        query = query.filter(CinderEventsProcessing.event_time <= end_time)
        if unhandled_only:
            query = query.filter(or_(
                CinderEventsProcessing.notification_status == '',
                CinderEventsProcessing.notification_status == constants.STATE_NEW,
                CinderEventsProcessing.notification_status == constants.STATE_RUNNING
            ))
        return query.all()


# update cinder event status
def update_cinder_processing_event(event_uuid, notification_uuid,
                                 notification_created,
                                 notification_status,
                                 error_state=''):
    """Update cinder processing event status.

    :param event_uuid: UUID of the event
    :param notification_uuid: UUID of the notification
    :param notification_created: When notification was created
    :param notification_status: Status of the notification
    :param error_state: Error state if any
    :return: Updated event object
    """
    with dbsession() as session:
        query = session.query(CinderEventsProcessing)
        query = query.filter(CinderEventsProcessing.event_uuid == event_uuid)
        event = query.first()
        if event:
            event.notification_status = notification_status
            event.notification_updated = datetime.datetime.utcnow()
            if error_state:
                event.error_state = error_state
            session.add(event)
            session.commit()
            return event
        return None
