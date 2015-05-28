# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import asyncio
from hbmqtt.utils import (
    bytes_to_hex_str,
    hex_to_int,
)
from hbmqtt.message import Message
from hbmqtt.streams.errors import FixedHeaderException

class BaseStream:
    def __init__(self):
        pass

    def decode(self, reader):
        b1 = yield from reader.read(1)
        msg_type = BaseStream.get_message_type(b1)
        (dup_flag, qos, retain_flag) = BaseStream.get_flags(b1)
        remain_length = yield from self.decode_remaining_length(b1, reader)
        return Message(msg_type, remain_length, dup_flag, qos, retain_flag)

    @staticmethod
    def get_message_type(byte):
        return (hex_to_int(byte) & 0xf0) >> 4

    @staticmethod
    def get_flags(data):
        byte = hex_to_int(data)
        b3 = True if (byte & 0x08) >> 3 else False
        b21 = (byte & 0x06) >> 1
        b0 = True if (byte & 0x01) else False
        return b3, b21, b0

    @asyncio.coroutine
    def decode_remaining_length(self, reader):
        multiplier = 1
        value = 0
        length_bytes = b''
        while True:
            encoded_byte = yield from reader.read(1)
            length_bytes += encoded_byte
            int_byte = hex_to_int(encoded_byte)
            value += (int_byte & 0x7f) * multiplier
            if (int_byte & 0x80) == 0:
                break
            else:
                multiplier *= 128
                if multiplier > 128*128*128:
                    raise FixedHeaderException("Invalid remaining length bytes:%s" % bytes_to_hex_str(length_bytes))
        return value
