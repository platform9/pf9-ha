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

from datetime import datetime
import uuid
import logging
from shared.constants import LOGGER_PREFIX

LOG = logging.getLogger(LOGGER_PREFIX + __name__)

class MessageBase(dict):
    def __init__(self, type, *args, **kwargs):
        id = str(uuid.uuid4())
        timestamp = datetime.utcnow()
        self._type = type
        self._id = id
        self._timestamp = timestamp
        str_timestamp = datetime.strftime(self._timestamp, '%Y-%m-%d %H:%M:%S')
        dict.__init__(self,
                      type=self._type,
                      id=self._id,
                      timestamp=str_timestamp,
                      *args,
                      **kwargs)

    def type(self):
        return self._type

    def id(self):
        return self._id

    def timestamp(self):
        return self._timestamp
