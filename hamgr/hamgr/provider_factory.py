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

import logging
from ConfigParser import ConfigParser
from hamgr import app
from flask import g
from shared.constants import LOGGER_PREFIX

LOG = logging.getLogger(LOGGER_PREFIX + __name__)


def ha_provider():
    with app.app_context():
        provider = getattr(g, '_ha_provider', None)
        if provider is None:
            # TODO: Make this part of config
            provider_name = 'nova'
            pkg = __import__('hamgr.providers.%s' % provider_name)
            conf = ConfigParser()
            conf.read(['/etc/pf9/hamgr/hamgr.conf'])
            module = getattr(pkg.providers, provider_name)
            provider = module.get_provider(conf)
            g._ha_provider = provider
        LOG.info('ha provider : %s', str(provider))
        return provider


def db_provider():
    with app.app_context():
        provider = getattr(g, '_db_provider', None)
        if provider is None:
            pkg = __import__('hamgr')
            conf = ConfigParser()
            conf.read(['/etc/pf9/hamgr/hamgr.conf'])
            provider = getattr(pkg.db, 'api')
            provider.init(conf)
            g._db_provider = provider
        LOG.info('db provider : %s', str(provider))
        return provider
