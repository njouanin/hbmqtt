# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import asyncio

from hbmqtt.mqtt.packet import MQTTPacket, MQTTFixedHeader, UNSUBSCRIBE, PacketIdVariableHeader, MQTTPayload, MQTTVariableHeader
from hbmqtt.errors import HBMQTTException, NoDataException
from hbmqtt.codecs import decode_string, encode_string


class UnubscribePayload(MQTTPayload):

    __slots__ = ('topics',)

    def __init__(self, topics=[]):
        super().__init__()
        self.topics = topics

    def to_bytes(self, fixed_header: MQTTFixedHeader, variable_header: MQTTVariableHeader):
        out = b''
        for topic in self.topics:
            out += encode_string(topic)
        return out

    @classmethod
    @asyncio.coroutine
    def from_stream(cls, reader: asyncio.StreamReader, fixed_header: MQTTFixedHeader,
                    variable_header: MQTTVariableHeader):
        topics = []
        payload_length = fixed_header.remaining_length - variable_header.bytes_length
        read_bytes = 0
        while read_bytes < payload_length:
            try:
                topic = yield from decode_string(reader)
                topics.append(topic)
                read_bytes += 2 + len(topic.encode('utf-8'))
            except NoDataException:
                break
        return cls(topics)


class UnsubscribePacket(MQTTPacket):
    VARIABLE_HEADER = PacketIdVariableHeader
    PAYLOAD = UnubscribePayload

    def __init__(self, fixed: MQTTFixedHeader=None, variable_header: PacketIdVariableHeader=None, payload=None):
        if fixed is None:
            header = MQTTFixedHeader(UNSUBSCRIBE, 0x02)  # [MQTT-3.10.1-1]
        else:
            if fixed.packet_type is not UNSUBSCRIBE:
                raise HBMQTTException("Invalid fixed packet type %s for UnsubscribePacket init" % fixed.packet_type)
            header = fixed

        super().__init__(header)
        self.variable_header = variable_header
        self.payload = payload

    @classmethod
    def build(cls, topics, packet_id):
        v_header = PacketIdVariableHeader(packet_id)
        payload = UnubscribePayload(topics)
        return UnsubscribePacket(variable_header=v_header, payload=payload)
