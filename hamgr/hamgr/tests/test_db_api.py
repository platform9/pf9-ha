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

from hamgr.db import api as db_api
from shared import constants

import mock

from six.moves.configparser import ConfigParser


class DbAPITest(unittest.TestCase):

    def setUp(self):
        config = ConfigParser()
        config.add_section('database')
        config.set('database', 'sqlconnectURI', 'sqlite://')
        config.set('database', 'sqlite_synchronous', 'False')

        db_api.init(config)

        db_api.Base.metadata.create_all(db_api._engine)


    def tearDown(self):
        db_api.Base.metadata.drop_all(db_api._engine)


    def test_create_then_query_cluster_with_name(self):
        name='23'
        task_state=constants.TASK_CREATING
        cluster = db_api.create_cluster_if_needed(name, task_state)
        self.assertIsNotNone(cluster)
        self.assertTrue(name==cluster.name)
        self.assertTrue((task_state==cluster.task_state))
        record = db_api.get_cluster(name)
        self.assertIsNotNone(record)

    def test_create_delete_then_query_cluster_with_name(self):
        name = '23'
        task_state = constants.TASK_CREATING
        cluster = db_api.create_cluster_if_needed(name, task_state)
        self.assertIsNotNone(cluster)
        self.assertTrue(name == cluster.name)
        self.assertTrue((task_state == cluster.task_state))
        db_api.update_cluster(cluster.id, False)
        record = db_api.get_cluster(name, read_deleted=False)
        self.assertIsNotNone(record)
        record = db_api.get_cluster(name, read_deleted=True)
        self.assertIsNotNone(record)
        self.assertTrue(record.deleted != 0)

    def test_get_all_active_clusters(self):
        name = '23'
        task_state = constants.TASK_CREATING
        cluster = db_api.create_cluster_if_needed(name, task_state)
        self.assertIsNotNone(cluster)
        self.assertTrue(name == cluster.name)
        self.assertTrue((task_state == cluster.task_state))
        db_api.update_cluster(cluster.id, True)
        db_api.update_request_status(cluster.id, constants.HA_STATE_ENABLED)
        active_clusters = db_api.get_all_active_clusters()
        self.assertIsNotNone(active_clusters)
        self.assertTrue(1 == len(active_clusters))
        db_api.update_cluster(cluster.id, False)
        db_api.update_request_status(cluster.id, constants.HA_STATE_DISABLED)
        active_clusters = db_api.get_all_active_clusters()
        self.assertIsNotNone(active_clusters)
        self.assertTrue(0 == len(active_clusters))
