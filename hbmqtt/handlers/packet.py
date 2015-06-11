# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.

import asyncio
import abc
import sys
import logging
from hbmqtt.messages.packet import MQTTPacket, MQTTHeader, PacketType
from hbmqtt.handlers.utils import int_to_bytes, bytes_to_int, read_or_raise, bytes_to_hex_str
from hbmqtt.handlers.errors import CodecException, HandlerException
from hbmqtt.session import Session
from hbmqtt.errors import MQTTException

if sys.version_info >= (3,4):
    import asyncio.ensure_future as async
else:
    import asyncio.async as async


class PacketHandler(metaclass=abc.ABCMeta):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.handled_packet_type = None

    @staticmethod
    @asyncio.coroutine
    def receive_next_packet_header(session: Session) -> MQTTHeader:
        next_packet_header = yield from PacketHandler._read_packet_header(session)
        return next_packet_header

    @asyncio.coroutine
    def receive_packet(self, fixed: MQTTHeader, session: Session) -> MQTTPacket:
        if fixed.packet_type is not self.handled_packet_type:
            raise HandlerException("Incompatible packet type '%s' with this handler" % fixed.packet_type.value)

        variable_header = yield from self._decode_variable_header(fixed, session)
        payload = yield from self._decode_payload(fixed, variable_header, session)
        return self._build_packet(fixed, variable_header, payload)

    @asyncio.coroutine
    def send_packet(self, packet: MQTTPacket, session: Session):
        encoders = [
            async(self._encode_variable_header(packet.variable_header)),
            async(self._encode_payload(packet.payload)),

        ]
        (encoded_variable_header, encoded_payload) = yield from asyncio.gather(encoders)
        packet.fixed_header.remaining_length = len(encoded_variable_header) + len(encoded_payload)
        encoded_fixed_header = yield from self._encode_fixed_header(packet.fixed_header)

        session.writer.write(encoded_fixed_header)
        session.writer.write(encoded_variable_header)
        session.writer.write(encoded_payload)
        yield from session.writer.drain()

    @staticmethod
    @asyncio.coroutine
    def _read_packet_header(session: Session) -> MQTTHeader:
        """
        Read and decode MQTT message fixed header from stream
        :return: FixedHeader instance
        """
        def decode_message_type(byte):
            byte_type = (bytes_to_int(byte) & 0xf0) >> 4
            return PacketType(byte_type)

        def decode_flags(data):
            byte = bytes_to_int(data)
            return byte & 0x0f

        @asyncio.coroutine
        def decode_remaining_length():
            """
            Decode message length according to MQTT specifications
            :return:
            """
            multiplier = 1
            value = 0
            length_bytes = b''
            while True:
                encoded_byte = yield from read_or_raise(session.reader, 1)
                length_bytes += encoded_byte
                int_byte = bytes_to_int(encoded_byte)
                value += (int_byte & 0x7f) * multiplier
                if (int_byte & 0x80) == 0:
                    break
                else:
                    multiplier *= 128
                    if multiplier > 128*128*128:
                        raise MQTTException("Invalid remaining length bytes:%s" % bytes_to_hex_str(length_bytes))
            return value

        b1 = yield from read_or_raise(session.reader, 1)
        msg_type = decode_message_type(b1)
        if msg_type is PacketType.RESERVED_0 or msg_type is PacketType.RESERVED_15:
            raise MQTTException("Usage of control packet type %s is forbidden" % msg_type)
        flags = decode_flags(b1)

        remain_length = yield from decode_remaining_length()
        return MQTTHeader(msg_type, flags, remain_length)

    @asyncio.coroutine
    def _encode_fixed_header(self, header : MQTTHeader) -> bytes:
        def encode_remaining_length(length: int):
            encoded = b''
            while True:
                length_byte = length % 0x80
                length //= 0x80
                if length > 0:
                    length_byte |= 0x80
                encoded += int_to_bytes(length_byte)
                if length <= 0:
                    break
            return encoded

        out = b''
        packet_type = 0
        try:
            packet_type = (header.packet_type.value << 4) | header.flags
            out += int_to_bytes(packet_type)
        except OverflowError:
            raise CodecException('packet_size encoding exceed 1 byte length: value=%d', packet_type)

        try:
            encoded_length = encode_remaining_length(header.remaining_length)
            out += encoded_length
        except OverflowError:
            raise CodecException('message length encoding exceed 1 byte length: value=%d', header.remaining_length)

        return out

    @abc.abstractmethod
    @asyncio.coroutine
    def _encode_variable_header(self, variable) -> bytes:
        pass

    @abc.abstractmethod
    @asyncio.coroutine
    def _encode_payload(self, payload) -> bytes:
        pass

    @abc.abstractmethod
    @asyncio.coroutine
    def _decode_variable_header(self, fixed: MQTTHeader, session: Session):
        pass

    @abc.abstractmethod
    @asyncio.coroutine
    def _decode_payload(self, fixed: MQTTHeader, variable_header, session: Session):
        pass

    @abc.abstractmethod
    @asyncio.coroutine
    def _build_packet(self, fixed: MQTTHeader, variable_header, payload):
        pass