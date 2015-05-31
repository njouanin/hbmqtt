# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import asyncio

from hbmqtt.codecs.utils import (
    bytes_to_hex_str,
    bytes_to_int,
    decode_string,
    read_or_raise,
    encode_string,
    int_to_bytes,
)
from hbmqtt.message import MQTTHeader, ConnectMessage
from hbmqtt.codecs.errors import CodecException, NoDataException
from hbmqtt.codecs.header import MQTTHeaderCodec


class ConnectException(CodecException):
    pass


class ConnectCodec:
    @staticmethod
    @asyncio.coroutine
    def decode(header: MQTTHeader, reader):
        if header.flags:
            raise ConnectException("[MQTT-2.2.2-1] Header flags reserved for future use")

        # Read CONNECT header
        #  protocol name
        protocol_name = yield from decode_string(reader)
        if protocol_name != "MQTT":
            raise ConnectException('[MQTT-3.1.2-1] Incorrect protocol name: "%s"' % protocol_name)

        # protocol level (only MQTT 3.1.1 supported)
        protocol_level_byte = yield from read_or_raise(reader, 1)
        protocol_level = bytes_to_int(protocol_level_byte)
        if protocol_level != 4:
            raise ConnectException(
                '[MQTT-3.1.2-2] Unsupported protocol level %s' % bytes_to_hex_str(protocol_level_byte))

        # flags
        flags_byte = yield from read_or_raise(reader, 1)
        flags = bytes_to_int(flags_byte)
        if flags & 0x01:
            raise ConnectException('[MQTT-3.1.2-3] CONNECT reserved flag must be set to 0')

        # keep-alive
        keep_alive_byte = yield from read_or_raise(reader, 2)
        keep_alive = bytes_to_int(keep_alive_byte)

        message = ConnectMessage(header, flags, keep_alive, protocol_name, protocol_level)

        # Read Payload
        #  Client identifier
        try:
            message.client_id = yield from decode_string(reader)
        except NoDataException:
            raise ConnectException('[[MQTT-3.1.3-3]] Client identifier must be present')

        # Read will topic, username and password
        if message.is_will_flag():
            try:
                message.will_topic = yield from decode_string(reader)
                message.will_message = yield from decode_string(reader)
            except NoDataException:
                raise ConnectException('will flag set, but will topic/message not present in payload')

        if message.is_user_name_flag():
            try:
                message.user_name = yield from decode_string(reader)
            except NoDataException:
                raise ConnectException('username flag set, but username not present in payload')

        if message.is_password_flag():
            try:
                message.password = yield from decode_string(reader)
            except NoDataException:
                raise ConnectException('password flag set, but password not present in payload')

        return message

    @staticmethod
    def encode(message: ConnectMessage, with_header=False) -> bytes:
        out = b''

        # Write CONNECT variable header
        # Protocol name
        out += encode_string(message.proto_name)
        # Protocol level
        out += int_to_bytes(message.proto_level)
        # flags
        out += int_to_bytes(message.flags)
        # keep alive
        out += int_to_bytes(message.keep_alive, 2)

        # Write payload
        # Client identifier
        out += encode_string(message.client_id)
        # Will topic / message
        if message.is_will_flag():
            out += encode_string(message.will_topic)
            out += encode_string(message.will_message)
        # username
        if message.is_user_name_flag():
            out += encode_string(message.user_name)
        # password
        if message.is_password_flag():
            out += encode_string(message.password)

        message.mqtt_header.remaining_length = len(out)
        if with_header:
            out = MQTTHeaderCodec.encode(message.mqtt_header) + out
        return out
