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

from shared.messages.message_base import MessageBase
from shared.messages import message_types as message_types


class ConsulRefreshResponse(MessageBase):
    def __init__(self, request_id, status, report, message, *args, **kwargs):
        self._req_id = request_id
        self._status = status
        self._report = report
        self._message = message
        super(ConsulRefreshResponse, self).__init__(type=message_types.MSG_CONSUL_REFRESH_RESPONSE,
                                                    req_id=self._req_id,
                                                    status=self._status,
                                                    report = self._report,
                                                    message=self._message,
                                                    *args,
                                                    **kwargs)

    def status(self):
        return self._status

    def req_id(self):
        return self._req_id

    def message(self):
        return self._message

    def report(self):
        return self._report
