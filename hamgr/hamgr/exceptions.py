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

class AggregateNotFound(Exception):
    def __init__(self, aggregate):
        message = 'Aggregate %s not found' % aggregate
        super(AggregateNotFound, self).__init__(message)


class ClusterExists(Exception):
    def __init__(self, cluster):
        message = 'Cluster %s exists' % cluster
        super(ClusterExists, self).__init__(message)


class ClusterNotFound(Exception):
    def __init__(self, cluster):
        message = 'Cluster %s not found' % cluster
        super(ClusterNotFound, self).__init__(message)

class ClusterBusy(Exception):
    def __init__(self, cluster, task):
        message = 'Cluster %s is running task %s' % (cluster, task)
        super(ClusterBusy, self).__init__(message)

class HostPartOfCluster(Exception):
    def __init__(self, host, cluster_id):
        message = 'Host %s already in cluster %d' % (host, cluster_id)
        super(HostPartOfCluster, self).__init__(message)


class HostNotFound(Exception):
    def __init__(self, host):
        message = 'Host %s not found in nova' % host
        super(HostNotFound, self).__init__(message)


class InsufficientHosts(Exception):
    def __init__(self, expected=3):
        message = 'Insufficient hosts to form a cluster. Atleast %d are neeeded' % expected
        super(InsufficientHosts, self).__init__(message)


class RoleConvergeFailed(Exception):
    def __init__(self, host):
        message = 'Host %s failed to converge' % host
        super(RoleConvergeFailed, self).__init__(message)


class UpdateConflict(Exception):
    def __init__(self, cluster, old_task, new_task):
        message = 'Cluster %s has %s task already running. Failed to update' \
                  ' task to %s' % (cluster, old_task, new_task)
        super(UpdateConflict, self).__init__(message)


class InvalidTaskState(Exception):
    def __init__(self, state):
        message = '%s is not a valid task state' % state
        super(InvalidTaskState, self).__init__(message)

class SegmentNotFound(Exception):
    def __init__(self, name):
        message = 'Segment %s was not found' % name
        super(SegmentNotFound, self).__init__(message)
