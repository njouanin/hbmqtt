__author__ = 'nico'

import abc
import asyncio
from hbmqtt.handlers.packet import PacketHandler
from hbmqtt.handlers.errors import HandlerException, NoDataException
from hbmqtt.messages.packet import MQTTHeader, PacketType
from hbmqtt.messages.connect import ConnectPacket, ConnectVariableHeader, ConnectPayload
from hbmqtt.errors import MQTTException
from hbmqtt.session import Session
from hbmqtt.handlers.utils import (
    read_or_raise,
    bytes_to_int,
    decode_string,
    int_to_bytes,
    encode_string
)
class ConnectHandler(PacketHandler):
    def __init__(self):
        super().__init__()

    @asyncio.coroutine
    def send_request(self, session: Session):
        vh = ConnectVariableHeader()
        payload = ConnectPayload()

        vh.keep_alive = session.keep_alive
        vh.clean_session_flag = session.clean_session
        vh.will_retain_flag = session.will_retain
        payload.client_id = session.client_id

        if session.username:
            vh.username_flag = True
            payload.username = session.username
        else:
            vh.username_flag = False

        if session.password:
            vh.password_flag = True
            payload.password = session.password
        else:
            vh.password_flag = False
        if session.will_flag:
            vh.will_flag = True
            vh.will_qos = session.will_qos
            payload.will_message = session.will_message
            payload.will_topic = session.will_topic
        else:
            vh.will_flag = False

        packet = ConnectPacket(vh, payload)
        yield from self.send_packet(packet, session)
        return packet

    @asyncio.coroutine
    def receive_request(self, session: Session):
        pass

    @asyncio.coroutine
    def _encode_variable_header(self, variable_header: ConnectVariableHeader) -> bytes:
        out = b''

        # Protocol name
        out += encode_string(variable_header.proto_name)
        # Protocol level
        out += int_to_bytes(variable_header.proto_level)
        # flags
        out += int_to_bytes(variable_header.flags)
        # keep alive
        out += int_to_bytes(variable_header.keep_alive, 2)

        return out

    @asyncio.coroutine
    def _decode_variable_header(self, fixed: MQTTHeader, session: Session):
        #  protocol name
        protocol_name = yield from decode_string(session.reader)
        if protocol_name != "MQTT":
            raise MQTTException('[MQTT-3.1.2-1] Incorrect protocol name: "%s"' % protocol_name)

        # protocol level (only MQTT 3.1.1 supported)
        protocol_level_byte = yield from read_or_raise(session.reader, 1)
        protocol_level = bytes_to_int(protocol_level_byte)

        # flags
        flags_byte = yield from read_or_raise(session.reader, 1)
        flags = bytes_to_int(flags_byte)
        if flags & 0x01:
            raise MQTTException('[MQTT-3.1.2-3] CONNECT reserved flag must be set to 0')

        # keep-alive
        keep_alive_byte = yield from read_or_raise(session.reader, 2)
        keep_alive = bytes_to_int(keep_alive_byte)

        return ConnectVariableHeader(flags, keep_alive, protocol_name, protocol_level)

    @asyncio.coroutine
    def _decode_payload(self, fixed: MQTTHeader, variable_header: ConnectVariableHeader, session: Session):
        payload = ConnectPayload()
        #  Client identifier
        try:
            payload.client_id = yield from decode_string(session.reader)
        except NoDataException:
            raise MQTTException('[[MQTT-3.1.3-3]] Client identifier must be present')

        # Read will topic, username and password
        if variable_header.will_flag:
            try:
                payload.will_topic = yield from decode_string(session.reader)
                payload.will_message = yield from decode_string(session.reader)
            except NoDataException:
                raise MQTTException('will flag set, but will topic/message not present in payload')

        if variable_header.username_flag:
            try:
                payload.username = yield from decode_string(session.reader)
            except NoDataException:
                raise HandlerException('username flag set, but username not present in payload')

        if variable_header.password_flag:
            try:
                payload.password = yield from decode_string(session.reader)
            except NoDataException:
                raise HandlerException('password flag set, but password not present in payload')

        return payload

    @asyncio.coroutine
    def _encode_payload(self, payload) -> bytes:
        pass

    @asyncio.coroutine
    def _build_packet(self, fixed: MQTTHeader, variable_header, payload):
        if fixed.flags:
            raise MQTTException("[MQTT-2.2.2-1] Header flags reserved for future use")
        if fixed.packet_type is not PacketType.CONNECT:
            raise HandlerException("Incompatible packet type '%s'" % fixed.packet_type.value)
        packet = ConnectPacket(variable_header, payload)
        packet.fixed_header.remaining_length = fixed.remaining_length
        return packet

