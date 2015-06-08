# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
from enum import Enum

class MessageType(Enum):
    RESERVED_0 = 0
    CONNECT = 1
    CONNACK = 2
    PUBLISH = 3
    PUBACK = 4
    PUBREC = 5
    PUBREL = 6
    PUBCOMP = 7
    SUBSCRIBE = 8
    SUBACK = 9
    UNSUBSCRIBE = 10
    UNSUBACK = 11
    PINGREQ = 12
    PINGRESP = 13
    DISCONNECT = 14
    RESERVED_15 = 15


def get_message_type(byte):
    return MessageType(byte)

class MQTTHeader:
    def __init__(self, msg_type, flags=0, length=0):
        if isinstance(msg_type, int):
            enum_type = msg_type
        else:
            enum_type = get_message_type(msg_type)
        self.message_type = enum_type
        self.remaining_length = length
        self.flags = flags

class MQTTMessage:
    def __init__(self, header):
        # MQTT header
        self.mqtt_header = header

class ConnectMessage(MQTTMessage):
    def __init__(self, header: MQTTHeader, flags, keep_alive, remote_address=None, remote_port=None, proto_name='MQTT', proto_level=0x04):
        super().__init__(header)

        # Connect header
        self.proto_name = proto_name
        self.proto_level = proto_level
        self.flags = flags
        self.keep_alive = keep_alive
        self.client_id = None
        self.will_topic = None
        self.will_message = None
        self.user_name = None
        self.password = None

        # HBMQTT info
        self._remote_address = remote_address
        self._remote_port = remote_port

    def is_user_name_flag(self):
        return True if (self.flags & 0x80) else False

    def is_password_flag(self):
        return True if (self.flags & 0x40) else False

    def is_will_retain(self):
        return True if (self.flags & 0x20) else False

    def is_will_flag(self):
        return True if (self.flags & 0x04) else False

    def is_clean_session(self):
        return True if (self.flags & 0x02) else False

    def is_reserved_flag(self):
        return True if (self.flags & 0x01) else False

    def will_qos(self):
        return (self.flags & 0x18) >> 3


class ConnackMessage(MQTTMessage):
    def __init__(self, session_parent, return_code):
        header = MQTTHeader(MessageType.CONNACK)
        super().__init__(header)
        self.session_parent = session_parent
        self.return_code = return_code

    class ReturnCode(enum):
        CONNECTION_ACCEPTED = 0
        UNACCEPTABLE_PROTOCOL_VERSION = 1
        IDENTIFIER_REJECTED = 2
        SERVER_UNAVAILABLE = 3
        BAD_USERNAME_PASSWORD = 4
        NOT_AUTHORIZED = 5