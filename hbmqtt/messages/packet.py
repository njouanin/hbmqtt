# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
from enum import Enum

class PacketType(Enum):
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


def get_packet_type(byte):
    return PacketType(byte)

class MQTTHeader:
    def __init__(self, packet_type, flags=0, length=0):
        if isinstance(packet_type, int):
            enum_type = packet_type
        else:
            enum_type = get_packet_type(packet_type)
        self.packet_type = enum_type
        self.remaining_length = length
        self.flags = flags


class MQTTPacket:
    def __init__(self, fixed: MQTTHeader):
        self.fixed_header = fixed
        self.variable_header = None
        self.payload = None





class ConnackMessage(MQTTMessage):
    def __init__(self, session_parent, return_code):
        header = MQTTHeader(PacketType.CONNACK)
        super().__init__(header)
        self.session_parent = session_parent
        self.return_code = return_code

    class ReturnCode(Enum):
        CONNECTION_ACCEPTED = 0
        UNACCEPTABLE_PROTOCOL_VERSION = 1
        IDENTIFIER_REJECTED = 2
        SERVER_UNAVAILABLE = 3
        BAD_USERNAME_PASSWORD = 4
        NOT_AUTHORIZED = 5