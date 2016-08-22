#
# Copyright (c) 2016, Platform9 Systems. All Rights Reserved.
#


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


class HostPartOfCluster(Exception):
    def __init__(self, host, cluster_id):
        message = 'Host %s already in cluster %d' % (host, cluster_id)
        super(HostPartOfCluster, self).__init__(message)


class HostNotFound(Exception):
    def __init__(self, host):
        message = 'Host %s not found in nova' % host
        super(HostNotFound, self).__init__(message)


class InsufficientHosts(Exception):
    def __ini__(self, expected=3):
        message = 'Insufficient hosts to form a cluster. Atleast %d are neeeded' % expected
        super(InsufficientHosts, self).__init__(message)
