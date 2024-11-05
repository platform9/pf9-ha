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

from abc import ABCMeta
from abc import abstractmethod


class Provider(object):
    __metaclass__ = ABCMeta

    """Interface to HA manager provider."""
    @abstractmethod
    def get(self, availability_zone):
        """Get the HA config status for given availability_zone

        :param availability_zone: If none, returns all
        :return: 'enabled'/'disabled'/'not-applicable'
        """
        pass

    @abstractmethod
    def put(self, availability_zone, method):
        """Enable/disable HA for an availability_zone

        :param availability_zone:
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
