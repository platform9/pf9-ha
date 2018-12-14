from shared.messages import message_types as message_types
from shared.messages.message_base import MessageBase
from shared.messages.rebalance_request import ConsulRoleRebalanceRequest
from shared.messages.rebalance_response import ConsulRoleRebalanceResponse
from shared.messages.consul_request import ConsulRefreshRequest
from shared.messages.consul_response import ConsulRefreshResponse

MSG_CATEGORY_REQUEST = 'request'
MSG_CATEGORY_RESPONSE = 'response'

MSG_SCHEMAS = [
    # +------------------------+--------------------+---------------------+-----------------+
    # | message_type           | message_category   |  message_class      |   not_used      |
    # +------------------------+--------------------+---------------------+-----------------+
    (message_types.MSG_ROLE_REBALANCE_REQUEST,   MSG_CATEGORY_REQUEST,   ConsulRoleRebalanceRequest,   None),
    (message_types.MSG_ROLE_REBALANCE_RESPONSE,  MSG_CATEGORY_RESPONSE,  ConsulRoleRebalanceResponse,  None),
    (message_types.MSG_CONSUL_REFRESH_REQUEST,   MSG_CATEGORY_REQUEST,   ConsulRefreshRequest,         None),
    (message_types.MSG_CONSUL_REFRESH_RESPONSE,  MSG_CATEGORY_RESPONSE,  ConsulRefreshResponse,        None)
]


def is_validate(message_object, message_type, message_category):
    if message_category not in [MSG_CATEGORY_REQUEST, MSG_CATEGORY_RESPONSE]:
        return False
    request_schemas = [x for x in MSG_SCHEMAS if x[1] == message_category]
    for schema in request_schemas:
        if schema[0] == message_type and isinstance(message_object, schema[2]):
            return True
    return False


def valid_message_types():
    return [x[0] for x in MSG_SCHEMAS]

def valid_request_types():
    return [x[0] for x in MSG_SCHEMAS if x[1] == MSG_CATEGORY_REQUEST]

def valid_response_types():
    return [x[0] for x in MSG_SCHEMAS if x[1] == MSG_CATEGORY_RESPONSE]

def is_validate_request(message_object, message_type):
    return is_validate(message_object, message_type, MSG_CATEGORY_REQUEST)


def is_validate_response(message_object, message_type):
    return is_validate(message_object, message_type, MSG_CATEGORY_RESPONSE)


def is_allowed_message(message_object, message_type):
    for schema in MSG_SCHEMAS:
        if schema[0] == message_type and isinstance(message_object, schema[2]):
            return True
    return False
