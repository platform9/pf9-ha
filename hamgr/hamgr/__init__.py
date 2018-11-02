from flask import Flask

DEFAULT_CONF_FILE = '/etc/pf9/hamgr/hamgr.conf'
DEFAULT_LOG_FILE = '/var/log/pf9/hamgr/hamgr.log'
DEFAULT_ROTATE_COUNT = 10
DEFAULT_ROTATE_SIZE = 524288000
DEFAULT_LOG_LEVEL = "INFO"

app = Flask(__name__)
app.debug = True
