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


VALID_EVENT_TYPES = [ EVENT_HOST_UP, EVENT_HOST_DOWN ]


HANDLED_STATES = [ STATE_ABORTED, STATE_FINISHED ]