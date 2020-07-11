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

# according to https://docs.python.org/2/library/logging.html#module-logging
# the sub modules will inherit settings from root logger if the name
# is in the format 'parent.child', so here we set the 'parent' as 'vmha'
ROOT_LOGGER = 'vmha'
LOGGER_PREFIX = ROOT_LOGGER + '.'

# the host events
EVENT_HOST_UP = 'host-up'
EVENT_HOST_DOWN = 'host-down'
EVENT_HOST_ADDED = 'host-added'
EVENT_HOST_REMOVED = 'host-removed'
EVENT_CONSUL_INSPECT = 'consul-role-inspect'
EVENT_CONSUL_CHANGE = 'consul-role-change'

HOST_EVENTS = [EVENT_HOST_UP , EVENT_HOST_DOWN,
               EVENT_HOST_ADDED, EVENT_HOST_REMOVED,
               EVENT_CONSUL_INSPECT, EVENT_CONSUL_CHANGE]

# the status of processing events
STATE_ABORTED = 'aborted'
STATE_FINISHED = 'finished'
STATE_DUPLICATED = 'duplicated'

VALID_EVENT_TYPES = [EVENT_HOST_UP, EVENT_HOST_DOWN]

HANDLED_STATES = [STATE_ABORTED, STATE_FINISHED, STATE_DUPLICATED]

HA_STATE_REQUEST_ENABLE = 'request-enable'
HA_STATE_ENABLING = 'enabling'
HA_STATE_ENABLED = 'enabled'
HA_STATE_REQUEST_DISABLE = 'request-disable'
HA_STATE_DISABLING = 'disabling'
HA_STATE_DISABLED = 'disabled'
HA_STATE_ERROR = 'error'

HA_STATE_ALL = [HA_STATE_REQUEST_ENABLE, HA_STATE_ENABLING,
                HA_STATE_ENABLED, HA_STATE_REQUEST_DISABLE,
                HA_STATE_DISABLING, HA_STATE_DISABLED, HA_STATE_ERROR]

RPC_TASK_STATE_RUNNING = 'running'
RPC_TASK_STATE_ERROR = 'error'
RPC_TASK_STATE_ABORTED = 'aborted'
RPC_TASK_STATE_FINISHED = 'finished'

# Blank i.e. NULL task state means that the task was completed. Hence the
# valid task states are only creating, deleting, updating and error-removing.

TASK_CREATING = 'creating'
TASK_REMOVING = 'removing'
TASK_MIGRATING = 'migrating'
TASK_COMPLETED = None
TASK_ERROR_REMOVING = 'error-removing'
VALID_TASK_STATES = [TASK_CREATING, TASK_MIGRATING, TASK_REMOVING,
                     TASK_COMPLETED, TASK_ERROR_REMOVING]

CONSUL_ROLE_SERVER = 'server'
CONSUL_ROLE_CLIENT = 'client'

# Desired/maximum no. of hosts in a consul cluster running in server mode
SERVER_THRESHOLD = 5

PF9_DISABLED_REASON = 'Host disabled by PF9 HA manager'

