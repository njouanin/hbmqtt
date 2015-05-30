# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import asyncio

from hbmqtt.codecs.utils import (
    bytes_to_hex_str,
    bytes_to_int,
    read_string,
    read_or_raise,
)
from hbmqtt.message import FixedHeader, ConnectMessage
from hbmqtt.codecs.errors import CodecException


class ConnectException(CodecException):
    pass


class ConnectStream:
    def __init__(self):
        pass

    @asyncio.coroutine
    def decode(self, fixed_header: FixedHeader, reader):
        if fixed_header.flags:
            raise ConnectException("[MQTT-2.2.2-1] Header flags reserved for future use")
        message = ConnectMessage(fixed_header)
        protocol_name = yield from read_string(reader)
        if protocol_name is not 'MQTT':
            raise ConnectException('[MQTT-3.1.2-1] Incorrect protocol name')
        protocol_level_byte = read_or_raise(reader, 1)
        protocol_level = bytes_to_int(protocol_level_byte)
        if protocol_level != 4:
            raise ConnectException('Unsupported protocol level %s' % bytes_to_hex_str(protocol_level_byte))
