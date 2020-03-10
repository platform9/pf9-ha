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
from datetime import datetime
from shared.messages import message_types as message_types
from shared.messages.message_base import MessageBase
from shared.constants import LOGGER_PREFIX

LOG = logging.getLogger(LOGGER_PREFIX + __name__)


class ConsulRefreshRequest(MessageBase):
    def __init__(self, cluster, cmd, *args, **kwargs):
        self._cluster = cluster
        self._command = cmd
        super(ConsulRefreshRequest, self).__init__(type=message_types.MSG_CONSUL_REFRESH_REQUEST,
                                                   cluster=self._cluster,
                                                   command=cmd,
                                                   *args,
                                                   **kwargs)

    def command(self):
        return self._command

    def cluster(self):
        return self._cluster
