from shared.messages import message_types as message_types
from shared.messages.message_base import MessageBase


class ConsulRoleRebalanceRequest(MessageBase):
    def __init__(self, host_id, old_role, new_role, *args, **kwargs):
        self._host_id = host_id
        self._old_role = old_role
        self._new_role = new_role
        super(ConsulRoleRebalanceRequest, self).__init__(type=message_types.MSG_ROLE_REBALANCE_REQUEST,
                                                         host_id=self._host_id,
                                                         old_role=self._old_role,
                                                         new_role=self._new_role,
                                                         *args,
                                                         **kwargs)

    def host_id(self):
        return self._host_id

    def old_role(self):
        return self._old_role

    def new_role(self):
        return self._new_role