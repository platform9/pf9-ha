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

import datetime

from hamgr.common import masakari

import mock


class MasakariTest(unittest.TestCase):

    def _mock_post_request(*args, **kwargs):
        url = args[0]
        if url == "http://localhost:8080/masakari/v1/segments":
            mock_resp = mock.Mock()
            mock_resp.status_code = 200
            mock_resp.raise_for_status = lambda *args: None
            mock_resp.json = lambda *args: dict({"segment": {"uuid": "fake-uuid"}})
            return mock_resp
        elif url == "http://localhost:8080/masakari/v1/segments/fake-uuid/hosts":
            mock_resp = mock.Mock()
            mock_resp.status_code = 200
            mock_resp.raise_for_status = lambda *args: None
            mock_resp.json = lambda *args: dict({"status_code": 200})
            return mock_resp

    @mock.patch('hamgr.common.utils.get_token')
    @mock.patch('requests.get')
    def test_get_failover_segment(self, mock_get, mock_token):
        json = {"segments": [
            {
                "name": "fake-segments",
                "id": "fake-id"
            }
        ]}
        mock_resp = mock.Mock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = lambda *args: None
        mock_resp.json = lambda *args: dict(json)
        mock_get.return_value = mock_resp
        segment = masakari.get_failover_segment(mock_token, "fake-segments")
        assert segment is not None

    @mock.patch('requests.get')
    @mock.patch('hamgr.common.masakari.get_failover_segment')
    @mock.patch('hamgr.common.utils.get_token')
    def test_get_nodes_in_segment(self, mock_token, mock_get_failover_segment, mock_get):
        json = {"hosts": []}
        mock_resp = mock.Mock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = lambda *args: None
        mock_resp.json = lambda *args: dict(json)
        mock_get.return_value = mock_resp
        mock_get_failover_segment.return_value = {"name": "fake-segments", "uuid": "fake-uuid", "hosts": []}
        nodes = masakari.get_nodes_in_segment(mock_token, "fake-segments")
        assert nodes is not None

    @mock.patch('requests.post', side_effect=_mock_post_request)
    @mock.patch("hamgr.common.masakari.delete_failover_segment")
    @mock.patch("hamgr.common.masakari.get_failover_segment")
    @mock.patch('hamgr.common.utils.get_token')
    def test_create_failover_segment(self, mock_token, mock_get_failover_segment, mock_delete_failover_segment,
                                     mock_post):
        mock_get_failover_segment.return_value = None
        mock_delete_failover_segment.return_value = None
        masakari.create_failover_segment(mock_token, "fake-segment", [{"name": "fake-host-name"}])

    @mock.patch("requests.delete")
    @mock.patch("hamgr.common.masakari.get_nodes_in_segment")
    @mock.patch("hamgr.common.masakari.get_failover_segment")
    @mock.patch("hamgr.common.utils.get_token")
    def test_delete_failover_segment(self, mock_token, mock_get_failover_segment, mock_get_nodes_in_segment,
                                     mock_delete):
        mock_get_nodes_in_segment.return_value = [{"failover_segment_id": "fake-segment", "uuid": "fake-uuid"}]
        mock_get_failover_segment.return_value = {"uuid": "fake-uuid"}
        mock_resp = mock.Mock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = lambda *args: None
        mock_delete.return_value = mock_resp
        masakari.delete_failover_segment(mock_token, "fake-segment")

    @mock.patch("requests.post")
    @mock.patch("hamgr.common.utils.get_token")
    def test_create_notification(self, mock_token, mock_post):
        mock_resp = mock.Mock()
        mock_resp.status_code = 202
        mock_resp.raise_for_status = lambda *args: None
        mock_post.return_value = mock_resp
        masakari.create_notification(mock_token, "host-down", "fake-host-id", str(datetime.datetime.now().isoformat()),
                                     {})

    @mock.patch("requests.get")
    @mock.patch("hamgr.common.utils.get_token")
    def test_get_notifications(self, mock_token, mock_get):
        mock_resp = mock.Mock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = lambda *args: None
        mock_resp.json = lambda *args: dict({})
        mock_get.return_value = mock_resp
        resp = masakari.get_notifications(mock_token, "fake-host-id")
        assert resp is not None
