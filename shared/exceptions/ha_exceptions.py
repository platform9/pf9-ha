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


class AvailabilityZoneNotFound(Exception):
    def __init__(self, availability_zone):
        message = 'Availability Zone %s not found' % availability_zone
        super(AvailabilityZoneNotFound, self).__init__(message)


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
    def __init__(self, hosts):
        message = 'Hosts %s already in cluster' % hosts
        super(HostPartOfCluster, self).__init__(message)


class HostNotInCluster(Exception):
    def __init__(self, host, cluster):
        message = 'Hosts %s is not in cluster %s' % (host, str(cluster))
        super(HostNotInCluster, self).__init__(message)


class HostOffline(Exception):
    def __init__(self, host):
        message = 'Host %s is offline' % (host)
        super(HostOffline, self).__init__(message)


class HostNotFound(Exception):
    def __init__(self, host):
        message = 'Host %s not found in nova' % host
        super(HostNotFound, self).__init__(message)


class InvalidHostRoleStatus(Exception):
    def __init__(self, host):
        message = 'Host %s does not have converged role status.' % (host)
        super(InvalidHostRoleStatus, self).__init__(message)


class InvalidHypervisorRoleStatus(Exception):
    def __init__(self, host):
        message = 'Host %s does not have valid hypervisor role status' % host
        super(InvalidHypervisorRoleStatus, self).__init__(message)


class InsufficientHosts(Exception):
    def __init__(self, expected=4):
        message = 'Insufficient hosts to form a cluster. At least %d are ' \
            'needed' % expected
        super(InsufficientHosts, self).__init__(message)


class RoleConvergeFailed(Exception):
    def __init__(self, host):
        message = 'Host %s failed to converge' % host
        super(RoleConvergeFailed, self).__init__(message)


class RoleNotExists(Exception):
    def __init__(self, host, role):
        message = 'Role %s does not exist on Host %s ' % (role, host)
        super(RoleNotExists, self).__init__(message)


class RoleSettingsNotFound(Exception):
    def __init__(self, host, role, settings):
        message = 'Settings for Role %s for Host %s does not exist : %s ' % (role, host, settings)
        super(RoleSettingsNotFound, self).__init__(message)

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


class ClusterIpNotFound(Exception):
    def __init__(self, hostname):
        message = "Cluster IP for host %s not found" % hostname
        super(ClusterIpNotFound, self).__init__(message)


class HostsIpNotFound(Exception):
    def __init__(self, hosts):
        message = "IP for hosts %s not found" % str(hosts)
        super(HostsIpNotFound, self).__init__(message)


class ArgumentException(Exception):
    def __init__(self, message):
        message = 'Invalid argument: %s' % message
        super(ArgumentException, self).__init__(message)


class ConfigException(Exception):
    def __init__(self, message):
        message = 'Config error: %s' % message
        super(ConfigException, self).__init__(message)


class NoCommonSharedNfsException(Exception):
    def __init__(self, message):
        message = 'NFS mismatch: %s' % message
        super(NoCommonSharedNfsException, self).__init__(message)

