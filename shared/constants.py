"""
Copyright 2018 Platform9 Systems Inc.(http://www.platform9.com)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

# the host events
EVENT_HOST_UP = 'host-up'
EVENT_HOST_DOWN = 'host-down'

# the status of processing events
STATE_ABORTED = 'aborted'
STATE_FINISHED = 'finished'

VALID_EVENT_TYPES = [EVENT_HOST_UP, EVENT_HOST_DOWN]

HANDLED_STATES = [STATE_ABORTED, STATE_FINISHED]

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

REBALABCE_STATE_RUNNING = 'running'
REBALABCE_STATE_ERROR = 'error'
REBALANCE_STATE_ABORTED = 'aborted'
REBALANCE_STATE_FINISHED = 'finished'

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