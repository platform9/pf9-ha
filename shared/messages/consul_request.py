from datetime import datetime
from shared.messages import message_types as message_types
from shared.messages.message_base import MessageBase
import logging

LOG = logging.getLogger(__name__)


class ConsulRefreshRequest(MessageBase):
    def __init__(self, cmd, *args, **kwargs):
        self._command = cmd
        super(ConsulRefreshRequest, self).__init__(type=message_types.MSG_CONSUL_REFRESH_REQUEST,
                                                   command=cmd,
                                                   xtime=datetime.strftime(datetime.utcnow(), '%Y-%m-%d %H:%M:%S'),
                                                   *args,
                                                   **kwargs)

    def command(self):
        return self._command
