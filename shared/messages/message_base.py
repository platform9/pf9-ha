from datetime import datetime
import uuid
class MessageBase(dict):
    def __init__(self, type, payload, id=str(uuid.uuid4()),timestamp=datetime.utcnow(), *args, **kwargs):
        self._type = type
        self._id = id
        self._timestamp = timestamp
        str_timestamp = datetime.strftime(self._timestamp, '%Y-%m-%d %H:%M:%S')
        # extend the payload
        payload['id']= self._id
        payload['type']=self._type
        payload['timestamp']= str_timestamp
        self._payload = payload
        dict.__init__(self,
                      type=self._type,
                      payload=self._payload,
                      id=self._id,
                      timestamp=str_timestamp,
                      *args,
                      **kwargs)

    def type(self):
        return self._type

    def payload(self):
        return self._payload

    def id(self):
        return self._id

    def timestamp(self):
        return self._timestamp
