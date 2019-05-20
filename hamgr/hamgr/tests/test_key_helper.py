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

import unittest

class KeyHelperTest(unittest.TestCase):

    def test_16bytes_key_generation(self):
        def _get_uri(pwd):
            return "mysql://hamgr:%s@localhost:3306/hamgr" % pwd

        def _get_pwd(uri):
            pwd = ""
            if uri:
                idx = uri.find('@')
                if idx > 0:
                    pwd = uri[0:idx]
                    idx = pwd.rfind(':')
                    if idx > 0:
                        pwd = pwd[idx + 1:]
            return pwd

        def _get_key(name, seed):
            # the key is composed with starting magic code 'pf9-dc'
            # and the cluster name (host aggregate id), if longer
            # than require 16 bytes, then trim it, when shorter ,
            # append 0 until length is 16
            str_name = "-" + str(name)
            key = 'pf9-dc%s' % str_name
            if seed:
                key = seed
            # key needs to be 16 bytes
            total = len(key) + len(str_name)
            if total <= 16:
                key = key + str_name
            else:
                if len(key) > 16:
                    if len(str_name) > 16:
                        key = key[0:16]
                    else:
                        key = key[0:(16 - len(str_name))] + str_name
                else:
                    if len(str_name) < 16:
                        key = key[0:(16 - len(str_name))] + str_name

            needed = 16 - len(key)
            if needed <= 0:
                key = key[0:16]
            else:
                while len(key) < 16:
                    key = key + '-0'
                key = key[0:16]
            print(key)
            return key

        scenarios = [
            {
                'pwd':'abc',
                'name':1,
                'expect':'abc-1-0-0-0-0-0-'
            },
            {
                'pwd':'abcdefghijklmnop',
                'name':123,
                'expect':'abcdefghijkl-123'
            },
            {
                'pwd':'abcdefghijklmnop',
                'name':22,
                'expect':'abcdefghijklm-22'
            },
            {
                'pwd': 'abcdefghijklmnop',
                'name': 112233445566778899,
                'expect': 'abcdefghijklmnop'
            },
            {
                'pwd': 'abcdefghijklmnopqrs',
                'name': 112233445566778899,
                'expect': 'abcdefghijklmnop'
            },
            {
                'pwd': 'abcdefghijklmnopqrst',
                'name': 112233,
                'expect': 'abcdefghi-112233'
            },
            {
                'pwd': 'abcdefghijklmnop',
                'name': 112233,
                'expect': 'abcdefghi-112233'
            },
            {
                'pwd': 'abcdefghijklmnop',
                'name': 1122334455667788,
                'expect': 'abcdefghijklmnop'
            }
        ]

        for sc in scenarios:
            name = sc['name']
            seed = _get_pwd(_get_uri(sc['pwd']))
            self.assertTrue(seed == sc['pwd'])
            key = _get_key(name, seed)
            self.assertTrue(key == sc['expect'], 'key = %s, expect = %s' % (key, sc['expect']))
