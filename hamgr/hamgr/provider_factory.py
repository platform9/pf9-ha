#
# Copyright (c) 2016, Platform9 Systems. All Rights Reserved
#
import logging
from ConfigParser import ConfigParser
from hamgr import app
from flask import g

LOG = logging.getLogger(__name__)


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
