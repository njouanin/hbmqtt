# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import asyncio
from hbmqtt.utils import (
    bytes_to_hex_str,
    bytes_to_int,
    read_or_raise,
)
from hbmqtt.message import FixedHeader, MessageType
from hbmqtt.streams.errors import StreamException, NoDataException

class FixedHeaderException(StreamException):
    pass

class FixedHeaderStream:
    def __init__(self):
        pass

    def decode(self, reader) -> FixedHeader:
        b1 = yield from read_or_raise(reader, 1)
        msg_type = FixedHeaderStream.get_message_type(b1)
        if msg_type is MessageType.RESERVED_0 or msg_type is MessageType.RESERVED_15:
            raise FixedHeaderException("Usage of control packet type %s is forbidden" % msg_type)
        flags = FixedHeaderStream.get_flags(b1)

        remain_length = yield from self.decode_remaining_length(reader)
        return FixedHeader(msg_type, flags, remain_length)

    @staticmethod
    def get_message_type(byte):
        byte_type = (bytes_to_int(byte) & 0xf0) >> 4
        return MessageType(byte_type)

    @staticmethod
    def get_flags(data):
        byte = bytes_to_int(data)
        return byte & 0x0f

    @asyncio.coroutine
    def decode_remaining_length(self, reader):
        multiplier = 1
        value = 0
        length_bytes = b''
        while True:
            encoded_byte = yield from read_or_raise(reader, 1)
            length_bytes += encoded_byte
            int_byte = bytes_to_int(encoded_byte)
            value += (int_byte & 0x7f) * multiplier
            if (int_byte & 0x80) == 0:
                break
            else:
                multiplier *= 128
                if multiplier > 128*128*128:
                    raise FixedHeaderException("Invalid remaining length bytes:%s" % bytes_to_hex_str(length_bytes))
        return value
