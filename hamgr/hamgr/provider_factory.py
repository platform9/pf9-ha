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
from hamgr import app
from flask import g
from shared.constants import LOGGER_PREFIX
from hamgr.providers import get_provider_for_service

from six.moves.configparser import ConfigParser

LOG = logging.getLogger(LOGGER_PREFIX + __name__)


def ha_provider(service_type='nova'):
    with app.app_context():
        provider_key = f'_ha_{service_type}_provider'
        provider = getattr(g, provider_key, None)
        if provider is None:
            conf = ConfigParser()
            conf.read(['/etc/pf9/hamgr/hamgr.conf'])
            provider = get_provider_for_service(conf, service_type)
            setattr(g, provider_key, provider)
        return provider


def cinder_provider():
    return ha_provider(service_type='cinder')


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
        return provider
