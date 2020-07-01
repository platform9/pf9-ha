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

import os
import sys
import unittest
import uuid
import mock
import time
import json
from oslo_config import cfg

class HostManagerTest(unittest.TestCase):
    pf9_ha_config="""
[DEFAULT]
host = 1e730294-8921-4559-ae4a-a3f5ce2f5c36

[node]
cluster_ip = 10.80.86.23
ip_address = 10.80.86.23

[keystone_authtoken]
auth_uri = http://localhost:8080/keystone
insecure = True
admin_tenant_name = services
admin_password = elv28zLIzKumSkmp
admin_user = pf9-ha-slave

[consul]
join = 10.80.86.51,10.80.86.46,10.80.86.36,10.80.86.31,10.80.86.47,10.80.86.27,10.80.86.25,10.80.86.40,10.80.86.34,10.80.86.43,10.80.86.44,10.80.86.42,10.80.86.45,10.80.86.22,10.80.86.37,10.80.86.48,10.80.86.41,10.80.86.29,10.80.86.23,10.80.86.49,10.80.86.39,10.80.86.52,10.80.86.28,10.80.86.32,10.80.86.33,10.80.86.35,10.80.86.38,10.80.86.21,10.80.86.26,10.80.86.50,10.80.86.30,10.80.86.24
report_interval = 180
cluster_name = 3
verify_server_hostname = false
key_file_content = LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQpNSUlFcEFJQkFBS0NBUUVBbjRDUE1HZEhkNnAwa0xnUFpoa3RHOTBHbDR1ZS9MbDB5Rm9tYkZETitTSGI5UzJUClJVaWJ4WThxZmE1VWplRDFlQkNhRkNMOUZBMC9zS2pubWgrOCs1VlZGcFVGVUdDK2pkakZ1M3IrSG1xbEdVVngKdklSRjZ5dW9VNGlaSVhsRWtaMkVDZExTclVSWHF4Wjh6YjVUZU5OalEwZVhrMTMyWXpmcjd1eTdIZk9yNGdvRApSWjZ2Ym44M1NYVHdHY0plUUdUR3pJN3N2bExTNzRoNTAvZnJxREhkcjMrMWZ0VU9wV0g3Q3JhOHJBZlhoS0RhCkFvRjQ0Ty8zRWdHR0tBeHVWMDJDSVh5eXk0bTMwWlZlVnJ1c0lxZzJoMTVkbVV2YVkrUVR1UmhnNENyc21aNi8KZlBKdnBvTVptWkI3TXRmSXBPcVJNNUR1RjY3cEF1RUkxeWUvV1FJREFRQUJBb0lCQVFDWitQL0JLU0liaWNmbQo0RmUxSnI2ZDlEcmwxbG1PKy9TWmFEVkpRS1BsU09OT0JrWHhqd3NZcG9ETlBKblJNSVdsOXRqV2NZUS9kQjYwCmlnaXhoc3ZuVFp3TEphWXdsb084NkxMUXJnVmNGWFQxTlUxN1AzRkRlU3lRSHdBOENSWEJQLzV6Z1RueEcxVksKQ25aR0l1SHZkSmlSSFM1Y2kwdExNbk9tZkk2UmROOWFSUGJJcFFyYURieUhzYk5hZFVOclJISFFMREpaMHVyUApFeVZ5b3ZTcDBOMnpOQTRJYWJ6YThVampVa25aTHZPOFRPV3duYzFSR0RmcDB0elpuMS83WmFmL1hCYzB3amFTClVVZ2pMd2V3LzMwYktsUEhLSnZvdDZKeXJXeFozVGNicDNpUnlwVUJHeG5kbWVyMk1CQ2k2NXVtZDZTaUgwNWIKc0pIWVExMWxBb0dCQU5JNHBlb3g5VElSS2loOUhrM0N2Rm84Z1RWaWNGMkxqcFhHM2Y2VitkVCs3T2U1RmZHNApveENlQmhrd29FQUVvZDBTT3dLSzFjMVZ1d3htOGJOcTcvYjFzdlVYVGhsNXpCcVJRSkE3VDZLZktCNC9oMWNXCmxxMGJIOGhqVnNmODV4cHFCSkhSM3c5dkhabFp5dEhONllFNmFsdUdkYmEzTmdDKzZkNDE4Q3NEQW9HQkFNSTgKY0VEYTdtWXJlU3FJOXJkbHFrbG01UDMzZW8vV2FycDRVaWNtTnIzd2I4TWtnLzBzOEs1aEdkcEVNSXl3S3ZrVwpFelJsaWxmMzU2Yy84Y3k1UkNHbVRFNklxNW1tMXg4em1nYTRLMlFvbDNSbmE3L3A4NjJzWklxOTd1T21NWHVJCkF1Sk1NWVBKUUVrakt5SDlwZ2FneG5BYllDR2d4VjRTQnoyeGY4OXpBb0dCQUtMZW5yVG1oYmlIa2VrU251TFMKS0FtbGJObkdiWlljSkprb0hTQThZL1pBbDUwa1Nic2dPTDRNSUY5dHpBb3RUSmF4cENSaEdpcGU3Rzg4WnJDQgovbTZRaDFqWitIbEdZdnFHWk1ZYUhhVzV0MlJRQmZSVUhPTDY2OUhlSFFNT2pxSnBWeWIrdWRvRVZhTlU3UTFGCmdrN0x5bEVreUppS00zMjZiQWpzTXltdkFvR0FVZGRoZlJKQ2JTNVlLWUg0WXFJbHREUDB2TVh3RUhkS0ZUUHAKZWJGeVUybmh6Wm12TzVnWitYL1VndEZFbTZNSEdGa2kwbXNPZGE1eEgxbWtLcHpOaGxncHd0VjNhSkNTQ0FXWQpHc2l6RDhyQ3RqdDFmVEc0aVM4Z1ZnMWRnUEpmMnlzZCsvZ2F6T3FaZWJlbHp2YXZaQStPVFdKYmlRL1MyYVpECkFzMnRpdDBDZ1lBTXZJMVdxTkZqMlNyOGZrbVlYYlF0dmRHWUloQjRhdFo2N0l3V2pIZjR5R3ZMSHAvYlBwWHQKSVgvWU5wcmtlMUdsRStydTE2ZHBnV2g1MU9aRlk3d3F0Ym1rUDN0M3p6bms2ZlNhS2ZWWGY5THFmSC9IOSsxTQo0SzJ0NlU3bWZkalRudlh6U0lPQ1A2NVlwdFkwZjZXMmExeE1KSktNb1k2aCt6MENZSFlLa3c9PQotLS0tLUVORCBSU0EgUFJJVkFURSBLRVktLS0tLQo=
bootstrap_expect = 3
cert_file_content = LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUMzakNDQWNhZ0F3SUJBZ0lDRUFBd0RRWUpLb1pJaHZjTkFRRUZCUUF3RURFT01Bd0dBMVVFQXd3RlpXSnoKWTI4d0hoY05NVGt3TlRFMU1UTXdORFUzV2hjTk5EWXdPVEk1TVRNd05EVTNXakFqTVE0d0RBWURWUVFEREFWbApZbk5qYnpFUk1BOEdBMVVFQ2d3SWMyVnlkbWxqWlhNd2dnRWlNQTBHQ1NxR1NJYjNEUUVCQVFVQUE0SUJEd0F3CmdnRUtBb0lCQVFDZmdJOHdaMGQzcW5TUXVBOW1HUzBiM1FhWGk1Nzh1WFRJV2lac1VNMzVJZHYxTFpORlNKdkYKanlwOXJsU040UFY0RUpvVUl2MFVEVCt3cU9lYUg3ejdsVlVXbFFWUVlMNk4yTVc3ZXY0ZWFxVVpSWEc4aEVYcgpLNmhUaUpraGVVU1JuWVFKMHRLdFJGZXJGbnpOdmxONDAyTkRSNWVUWGZaak4rdnU3THNkODZ2aUNnTkZucTl1CmZ6ZEpkUEFad2w1QVpNYk1qdXkrVXRMdmlIblQ5K3VvTWQydmY3VisxUTZsWWZzS3RyeXNCOWVFb05vQ2dYamcKNy9jU0FZWW9ERzVYVFlJaGZMTExpYmZSbFY1V3U2d2lxRGFIWGwyWlM5cGo1Qk81R0dEZ0t1eVpucjk4OG0rbQpneG1aa0hzeTE4aWs2cEV6a080WHJ1a0M0UWpYSjc5WkFnTUJBQUdqTHpBdE1Ba0dBMVVkRXdRQ01BQXdDd1lEClZSMFBCQVFEQWdVZ01CTUdBMVVkSlFRTU1Bb0dDQ3NHQVFVRkJ3TUJNQTBHQ1NxR1NJYjNEUUVCQlFVQUE0SUIKQVFBU2k2eWR2eVVrVmFxRlp5RUZONlJWSjZPNXgza2xiYlJCT0twditPYzJhWjVwakhiSzFJbFk2STYxaHljOApwakJjL0ljTmFma256VnVjUmxLMlFGZGw0VzF2Rks4azJma1kyOFd1QXVXWmp6YzZqWHMrenZYRVM1NTAxMzlkCjg1NXczNXpBekNIV1IzNzBQNUVLdktNS0Y1OStZRUNaaWtUM3R3dTFydUZXa2FGZ0FwREhKM1V6YWZ6WGVIdGkKbzlGMHNYWXA0YnVJVkZkaGR5ajBoS1ZCMnFJUDRBNXdpcEsrdDViQ0xtcTUwTjljVVpuTjdlcVdFRCtLZzl0WAoyWkMrVFVRN0FQQlZvYjBsMm54NXdOU21mYWZINy9KeDdMeWU0azJIK1BtVDUrVThJOTAyVWFpTi9zVUlGRDNlClczU1dBNVNWVm9jS1l5ZEVzR25oQyt6bwotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg==
encrypt = cGY5LWRjLTMtMDAwMDAwMA==
ca_file_content = LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUN3RENDQWFpZ0F3SUJBZ0lKQUxhY3QzaHpKamZNTUEwR0NTcUdTSWIzRFFFQkJRVUFNQkF4RGpBTUJnTlYKQkFNTUJXVmljMk52TUI0WERURTVNRFV4TlRFek1EUTFNMW9YRFRRMk1Ea3lPVEV6TURRMU0xb3dFREVPTUF3RwpBMVVFQXd3RlpXSnpZMjh3Z2dFaU1BMEdDU3FHU0liM0RRRUJBUVVBQTRJQkR3QXdnZ0VLQW9JQkFRQzJhZHBFCktsbnZuUVNzQVVSUUYxc1NpcGtTaEl3TXd3cHoxUmJVOWhUd25KWUxzN2hPZ0gveGZnbmg3c3FvV0tZSjNCVEoKd2RXVk5ONFZrK1V3OGlPMVQrSzFBZG54cUlGTFo3eEd1ZHZkb0hRQ1d2emhIVE56c205L1gzQW1OSTNHSmNDagp3RUJSNE1JVUxYSFNZNzFWRkZSTkpLWGw2OHR4eS9JQUttQm5hME5WeWNRUksvZ0R4dHAvZ0ozKzNac1I5QmsxCk8wNHBOajNUOThhRVBPRFp0bG9YMkFpN0hNY3poeTQwOVgvSUlQVTFVRXc2b3dYQ05PRlY3YjJwMU51emR1TzcKQU1YN3h5UjR4L3k3YThlQk1hRmduY2l6eTB6aWt1aENCZmg1V21QdXQ0ZnZkQVN2b1owZGhqUFVVdzRNWkFuSQpJUjdmc3oyeEc1YngxUWwzQWdNQkFBR2pIVEFiTUF3R0ExVWRFd1FGTUFNQkFmOHdDd1lEVlIwUEJBUURBZ0VHCk1BMEdDU3FHU0liM0RRRUJCUVVBQTRJQkFRQjB0MGVMS3A0Uk05aWFBTWJHRG5HTVlvVjZ0SnhKYWlrNXY4YUsKQkJpTFA1RExua2NWeVhIalIyeEY5VXo3TWVtei9HeTFXM24rNlRRemJxNGhJVEIwSkdpQTYrbUR6bm5qZjRoSwpqU2szK1ZrZjhQSmZWYnNZRXlEbzJSQ3ZMc3Q1bHRSVGN4SVF6eExtY1dBbFpjRVhDWTc2clFOejRqRnA0SE5XCjdYRW1Kd2pyTWhIcUF6a0MxWFhMb0tReUVhMmJsWW9WeUZhaE1KTVJOaWlLeGNpZ0Y1dVpuaTVkZFZjQlJuWVQKNTcxOGV3L1EwQ0IrUG9GSWpnL2hLdEpLS05RSWtxZ2lKVmlzbVl4MXdxNXVVMEJnTmx4YnZCN3ZWNXZzNlpYNQptUzZmcnByaXAzWjExcms1SFpvS3NVM0h6UUhNeWVNU3E5aGd4aTMwbWlzVmc0MHEKLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQo=
verify_incoming = true
verify_outgoing = true

[consul_role_rebalance]
role_rebalance_enabled = True
amqp_routingkey_receiving = role-rebalance-request
amqp_virtualhost = /
amqp_user = hJcRVKhJLreHbxiQ
amqp_routingkey_sending = role-rebalance-response
amqp_exchange_name = consul-role-rebalance-exchange
amqp_port = 5672
amqp_password = x70y2eiFs9pYRvIL
amqp_host = localhost
amqp_exchange_type = direct
"""

    current_consul_settings= """
{
    "advertise_addr": "10.80.86.23",
    "bind_addr": "10.80.86.23",
    "bootstrap_expect": 3,
    "data_dir": "/opt/pf9/consul-data-dir/",
    "datacenter": "",
    "disable_remote_exec": true,
    "encrypt": "",
    "log_level": "info",
    "node_name": "1e730294-8921-4559-ae4a-a3f5ce2f5c36",
    "retry_join": [
        "10.80.86.51",
        "10.80.86.46",
        "10.80.86.36",
        "10.80.86.31",
        "10.80.86.47",
        "10.80.86.27",
        "10.80.86.25",
        "10.80.86.40",
        "10.80.86.34",
        "10.80.86.43",
        "10.80.86.44",
        "10.80.86.42",
        "10.80.86.45",
        "10.80.86.22",
        "10.80.86.37",
        "10.80.86.48",
        "10.80.86.41",
        "10.80.86.29",
        "10.80.86.23",
        "10.80.86.49",
        "10.80.86.39",
        "10.80.86.52",
        "10.80.86.28",
        "10.80.86.32",
        "10.80.86.33",
        "10.80.86.35",
        "10.80.86.38",
        "10.80.86.21",
        "10.80.86.26",
        "10.80.86.50",
        "10.80.86.30",
        "10.80.86.24"
    ],
    "server": true,
    "verify_incoming": false,
    "verify_outgoing": false,
    "verify_server_hostname": false
}
"""
    def __init__(self, *args, **kwargs):
        super(HostManagerTest, self).__init__(*args, **kwargs)
        with open("test.conf", "w") as fp:
            fp.write(self.pf9_ha_config)
        os.system("mkdir -p conf.d")
        with open("conf.d/server.json", "w") as fp:
            fp.write(self.current_consul_settings)

    def _logging_to_console(self, *args, **kwargs):
        if len(args) > 0:
            msg = args[0] % (args[1:])
            print(msg)

    def _get_conf(self):
        return self.local_cfg

    @mock.patch('logging.getLogger')
    @mock.patch('logging.config.dictConfig')
    @mock.patch('oslo_config.cfg.CONF')
    def setUp(self,
              mock_conf,
              mock_config_dictConfig,
              mock_logger
              ):

        self.local_cfg = cfg.ConfigOpts()
        self.local_cfg(['--config-file', 'test.conf'])
        conf_instance = mock_conf.return_value
        conf_instance.CONF = self.local_cfg

        logger_instance = mock_logger.return_value
        logger_instance.debug.side_effect = self._logging_to_console
        logger_instance.info.side_effect = self._logging_to_console
        logger_instance.warn.side_effect = self._logging_to_console
        logger_instance.exception.side_effect = self._logging_to_console

        mock_utils = mock.MagicMock()
        mock_consul_helper = mock.MagicMock()
        mock_consul_helper.get_ip_address.return_value = '10.80.86.23'
        mock_consul_helper.get_bind_address.return_value = "10.80.86.23"

        mock_utils.consul_helper = mock_consul_helper

        sys.modules['consul'] = mock.MagicMock()
        sys.modules['consul.Consul'] = mock.MagicMock()
        sys.modules['ha.utils'] = mock_utils
        sys.modules['ha.utils.consul_helper'] = mock_consul_helper

        #sys.modules['ha'] = mock.MagicMock()
        from ha.hostapp import manager
        self._manager = manager
        self._manager.PF9_CONSUL_CONF_DIR=""
        self._manager.CONF = self.local_cfg
        self._manager.setup_conf_options()


    def tearDown(self):
        paths = [
            {'path':'conf.d/server.json', 'type':'file'},
            {'path':'conf.d', 'type':'dir'},
            {'path':'test.conf', 'type':'file' }
        ]
        for x in paths:
            if os.path.exists(x['path']):
                if x['type'] == 'file':
                    os.system('rm -f %s' % x['path'])
                if x['type'] == 'dir':
                    os.system('rm -rf %s' % x['path'])

    def test_consul_config_needs_refresh(self):
        needed = self._manager.config_needs_refresh()
        assert needed is True
