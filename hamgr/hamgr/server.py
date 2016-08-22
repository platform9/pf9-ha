#!/usr/bin/env python

#
# Copyright (c) 2016, Platform9 Systems, All Rights Reserved.
#

from paste.deploy import loadapp
from eventlet import wsgi
import eventlet
import argparse, logging
import logging.handlers
import ConfigParser

eventlet.monkey_patch()


def _get_arg_parser():
    parser = argparse.ArgumentParser(description="High Availability Manager for VirtualMachines")
    parser.add_argument('--config-file', dest='config_file', default='/etc/pf9/hamgr/hamgr.conf')
    parser.add_argument('--paste-ini', dest='paste_file')
    return parser.parse_args()


def _configure_logging(conf):
    log_filename = conf.get("log", "location")
    logging.basicConfig(filename=log_filename,
                        level=logging.DEBUG,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M')
    handler = logging.handlers.RotatingFileHandler(
        log_filename, maxBytes=1024 * 1024 * 5, backupCount=5)
    logging.root.addHandler(handler)


def start_server(conf, paste_ini):
    _configure_logging(conf)
    if paste_ini:
        paste_file = paste_ini
    else:
        paste_file = conf.get("DEFAULT", "paste-ini")

    wsgi_app = loadapp('config:%s' % paste_file, 'main')
    wsgi.server(eventlet.listen(('', conf.getint("DEFAULT", "listen_port"))), wsgi_app)


if __name__ == '__main__':
    parser = _get_arg_parser()
    conf = ConfigParser.ConfigParser()
    with open(parser.config_file) as f:
        conf.readfp(f)
    start_server(conf, parser.paste_file)

