# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
from hbmqtt.handlers.packet import PacketHandler, MQTTHeader, PacketType
from hbmqtt.messages.connack import ConnackPacket, ConnackVariableHeader, ReturnCode
from hbmqtt.session import Session
from hbmqtt.handlers.utils import *
from hbmqtt.errors import MQTTException


class ConnackHandler(PacketHandler):
    def __init__(self):
        super().__init__()
        self.handled_packet_type = PacketType.CONNACK

    def _build_packet(self, fixed: MQTTHeader, variable_header, payload):
        if fixed.flags:
            raise MQTTException("[MQTT-2.2.2-1] Header flags reserved for future use")
        if payload:
            self.logger.warn("CONNACK handler ignores payload")
        packet = ConnackPacket(variable_header)
        packet.fixed_header.remaining_length = fixed.remaining_length
        return packet

    def _decode_payload(self, fixed: MQTTHeader, variable_header, session: Session):
        return None

    def _encode_payload(self, payload) -> bytes:
        self.logger.warn("CONNACK handler ignores payload")
        return b''

    def _encode_variable_header(self, variable: ConnackVariableHeader) -> bytes:
        out = b''

        # Connect acknowledge flags
        if variable.session_parent:
            out += '\x01'
        else:
            out += '\x00'
        # return code
        out += int_to_bytes(variable.return_code)

        return out

    def _decode_variable_header(self, fixed: MQTTHeader, session: Session):
        # Read flags
        connack_flags_bytes = yield from read_or_raise(session.reader, 1)
        connack_flags = bytes_to_int(connack_flags_bytes)
        if connack_flags & 0xfe:
            pass
        session_parent = True if connack_flags & 0x01 else False

        return_code_bytes = yield from read_or_raise(session.reader, 1)
        return_code = bytes_to_int(return_code_bytes)
        if return_code not in list(ReturnCode):
            raise MQTTException("[MQTT-3.2.2-6] Return code '%s' reserved for future use" % bytes_to_hex_str(return_code))

        return ConnackVariableHeader(session_parent, return_code)