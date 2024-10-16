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

from shared.messages import message_types as message_types
from shared.messages.message_base import MessageBase


class ConsulRoleRebalanceRequest(MessageBase):
    def __init__(self, cluster, host_id, old_role, new_role, *args, **kwargs):
        self._cluster = cluster
        self._host_id = host_id
        self._old_role = old_role
        self._new_role = new_role
        super(ConsulRoleRebalanceRequest, self).__init__(type=message_types.MSG_ROLE_REBALANCE_REQUEST,
                                                         cluster = self._cluster,
                                                         host_id=self._host_id,
                                                         old_role=self._old_role,
                                                         new_role=self._new_role,
                                                         *args,
                                                         **kwargs)

    def cluster(self):
        return self._cluster

    def host_id(self):
        return self._host_id

    def old_role(self):
        return self._old_role

    def new_role(self):
        return self._new_role