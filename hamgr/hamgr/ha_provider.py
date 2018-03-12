#
# Copyright (c) 2016, Platform9 Systems. All Rights Reserved
#
from ConfigParser import ConfigParser
from flask import g


def ha_provider():
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
    return provider

