from datetime import datetime
import uuid
import logging

LOG = logging.getLogger(__name__)

class MessageBase(dict):
    def __init__(self, type, *args, **kwargs):
        id = str(uuid.uuid4())
        timestamp = datetime.utcnow()
        self._type = type
        self._id = id
        self._timestamp = timestamp
        str_timestamp = datetime.strftime(self._timestamp, '%Y-%m-%d %H:%M:%S')
        dict.__init__(self,
                      type=self._type,
                      id=self._id,
                      timestamp=str_timestamp,
                      *args,
                      **kwargs)

    def type(self):
        return self._type

    def id(self):
        return self._id

    def timestamp(self):
        return self._timestamp
