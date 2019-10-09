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

from flask import Flask

DEFAULT_CONF_FILE = '/etc/pf9/hamgr/hamgr.conf'
DEFAULT_LOG_FILE = '/var/log/pf9/hamgr/hamgr.log'
DEFAULT_ROTATE_COUNT = 5
DEFAULT_ROTATE_SIZE = 10485760
DEFAULT_LOG_LEVEL = "INFO"

app = Flask(__name__)
app.debug = True
