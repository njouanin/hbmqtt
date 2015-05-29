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

class FixedHeader:
    def __init__(self, msg_type, flags, length):
        if isinstance(msg_type, int):
            enum_type = msg_type
        else:
            enum_type = get_message_type(msg_type)
        self.message_type = enum_type
        self.remainingLength = length
        self.flags = flags
