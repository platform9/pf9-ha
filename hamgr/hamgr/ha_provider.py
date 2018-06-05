#
# Copyright (c) 2016, Platform9 Systems. All Rights Reserved
#
import logging
from ConfigParser import ConfigParser
from flask import Flask
from flask import g

app = Flask(__name__)
LOG = logging.getLogger(__name__)

def ha_provider():
    with app.app_context():
        provider = getattr(g, '_provider', None)
        if provider is None:
            # TODO: Make this part of config
            provider_name = 'nova'
            pkg = __import__('hamgr.providers.%s' % provider_name)
            conf = ConfigParser()
            conf.read(['/etc/pf9/hamgr/hamgr.conf'])
            module = getattr(pkg.providers, provider_name)
            provider = module.get_provider(conf)
            g._provider = provider
        LOG.info('ha provider : %s', str(provider))
        return provider

