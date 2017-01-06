# Copyright (c) 2016 Platform9 Systems Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

TASK_CREATING = 'creating'
TASK_REMOVING = 'removing'
TASK_MIGRATING = 'migrating'
TASK_COMPLETED = None
TASK_ERROR_REMOVING = 'error-removing'

# Blank i.e. NULL task state means that the task was completed. Hence the
# valid task states are only creating, deleting, updating and error-removing.
VALID_TASK_STATES = [TASK_CREATING, TASK_MIGRATING, TASK_REMOVING,
                     TASK_COMPLETED, TASK_ERROR_REMOVING]
