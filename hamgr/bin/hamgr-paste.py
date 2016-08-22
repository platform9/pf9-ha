# Copyright (c) 2016 Platform9 Systems Inc.
# All Rights reserved

import paste.httpserver
import os.path

# startup script for paste server testing

# FIXME - would be nice to be able to provide the pecan config file in the paste ini.
this_dir = os.path.abspath(os.path.dirname(__file__))
pecan_config = os.path.join(this_dir, '..', 'hostmgr', 'tests', 'config.py')
paste_ini = os.path.join(this_dir, '..', 'hostmgr', 'tests', 'hostmgr-paste.ini')
application = paste.deploy.loadapp('config:%s' % paste_ini,
                                   global_conf={'config': pecan_config})

paste.httpserver.serve(application, port=9083)
