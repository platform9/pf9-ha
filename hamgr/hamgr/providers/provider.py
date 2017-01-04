#
# Copyright (c) 2016, Platform9 Systems. All Rights Reserved.
#
from abc import ABCMeta
from abc import abstractmethod


class Provider(object):
    __metaclass__ = ABCMeta

    """
    Interface to HA manager provider.
    """
    @abstractmethod
    def get(self, aggregate_id):
        """
        Get the HA config status for given aggregate
        :param aggregate_id: If none, returns all
        :return: 'enabled'/'disabled'/'not-applicable'
        """
        pass

    @abstractmethod
    def put(self, aggregate_id, method):
        """
        Enable/disable HA for an aggregate
        :param aggregate_id:
        :param method: enable/disable
        :return:
        """
        pass

    @abstractmethod
    def host_up(self, event_details):
        pass

    @abstractmethod
    def host_down(self, event_details):
        pass
