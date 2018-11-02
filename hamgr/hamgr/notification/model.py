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


class Notification(dict):
    def __init__(self, action, target, identifier):
        self._action = action
        self._target = target
        self._identifier = identifier
        # make the Notification to be json serializable
        dict.__init__(self, action=self._action, target=self._target,
                      identifier=self._identifier)

    def action(self):
        return self._action

    def target(self):
        return self._target

    def identifier(self):
        return self._identifier
