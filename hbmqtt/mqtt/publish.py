# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
from hbmqtt.mqtt.packet import MQTTPacket, MQTTFixedHeader, PacketType, MQTTVariableHeader, MQTTPayload
from hbmqtt.errors import HBMQTTException, MQTTException
from hbmqtt.codecs import *


class PublishFixedHeader(MQTTFixedHeader):
    DUP_FLAG = 0x08
    RETAIN_FLAG = 0x01
    QOS_FLAG = 0x06

    def set_flags(self, dup_flag=False, qos=0, retain_flag=False):
        self.dup_flag = dup_flag
        self.retain_flag = retain_flag
        self.qos = qos

    def _set_flag(self, val, mask):
        if val:
            self.flags |= mask
        else:
            self.flags &= ~mask

    def _get_flag(self, mask):
        if self.flags & mask:
            return True
        else:
            return False

    @property
    def dup_flag(self) -> bool:
        return self._get_flag(self.DUP_FLAG)

    @dup_flag.setter
    def dup_flag(self, val: bool):
        self._set_flag(val, self.DUP_FLAG)

    @property
    def retain_flag(self) -> bool:
        return self._get_flag(self.RETAIN_FLAG)

    @retain_flag.setter
    def retain_flag(self, val: bool):
        self._set_flag(val, self.RETAIN_FLAG)

    @property
    def qos(self):
        return (self.flags & self.QOS_FLAG) >> 1

    @qos.setter
    def qos(self, val: int):
        self.flags &= (0x00 << 1)
        self.flags |= (val << 1)


class PublishVariableHeader(MQTTVariableHeader):
    def __init__(self, topic_name: str, packet_id: int=None):
        super().__init__()
        if '*' in topic_name:
            raise MQTTException("[MQTT-3.3.2-2] Topic name in the PUBLISH Packet MUST NOT contain wildcard characters.")
        self.topic_name = topic_name
        self.packet_id = packet_id

    def to_bytes(self):
        out = b''
        out += encode_string(self.topic_name)
        if self.packet_id is not None:
            out += int_to_bytes(self.packet_id, 2)
        return out

    @classmethod
    def from_stream(cls, reader: asyncio.StreamReader, fixed_header: PublishFixedHeader):
        topic_name = yield from decode_string(reader)
        if fixed_header.qos:
            packet_id = yield from decode_packet_id(reader)
        else:
            packet_id = None
        return cls(topic_name, packet_id)


class PublishPayload(MQTTPayload):
    def __init__(self, data: bytes=None):
        super().__init__()
        self.data = data

    def to_bytes(self, fixed_header: MQTTFixedHeader, variable_header: MQTTVariableHeader):
        return self.data

    @classmethod
    def from_stream(cls, reader: asyncio.StreamReader, fixed_header: MQTTFixedHeader,
                    variable_header: MQTTVariableHeader):
        data = yield from reader.read()
        return cls(data)


class PublishPacket(MQTTPacket):
    FIXED_HEADER = PublishFixedHeader
    VARIABLE_HEADER = PublishVariableHeader
    PAYLOAD = PublishPayload

    def __init__(self, fixed: PublishFixedHeader=None, variable_header: PublishVariableHeader=None, payload=None):
        if fixed is None:
            header = PublishFixedHeader(PacketType.PUBLISH, 0x00)
        else:
            if fixed.packet_type is not PacketType.PUBLISH:
                raise HBMQTTException("Invalid fixed packet type %s for PublishPacket init" % fixed.packet_type)
            header = fixed

        super().__init__(header)
        self.variable_header = variable_header
        self.payload = payload

    @classmethod
    def build(cls, topic_name: str, packet_id: int=None):
        v_header = PublishVariableHeader(topic_name, packet_id)
        return PublishPacket(variable_header=v_header)