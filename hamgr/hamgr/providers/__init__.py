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
from shared.constants import LOGGER_PREFIX

LOG = logging.getLogger(LOGGER_PREFIX + __name__)


def get_provider_for_service(config, service_type):
    """Get provider for a specific service type.

    :param config: Configuration object
    :param service_type: Service type (nova, cinder)
    :return: Provider instance
    """
    if service_type == 'nova':
        from hamgr.providers.nova import get_provider
        return get_provider(config)
    elif service_type == 'cinder':
        try:
            LOG.debug('Attempting to import cinder provider')
            from hamgr.providers.cinder import get_provider
            LOG.debug('Successfully imported cinder provider')
            provider = get_provider(config)
            LOG.debug('Successfully created cinder provider instance')
            return provider
        except Exception as e:
            LOG.exception('Error initializing cinder provider: %s', str(e))
            return None
    else:
        LOG.error('Unknown service type: %s', service_type)
        return None