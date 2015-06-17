# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
from hbmqtt.mqtt.packet import MQTTPacket, MQTTFixedHeader, PacketType, PacketIdVariableHeader, MQTTPayload, MQTTVariableHeader
from hbmqtt.errors import HBMQTTException, MQTTException
from hbmqtt.codecs import *


class SubscribePayload(MQTTPayload):
    def __init__(self, topics=[]):
        super().__init__()
        self.topics = topics

    def to_bytes(self, fixed_header: MQTTFixedHeader, variable_header: MQTTVariableHeader):
        out = b''
        for topic in self.topics:
            out += encode_string(topic['filter'])
            out += int_to_bytes(topic['qos'], 1)
        return out

    @classmethod
    @asyncio.coroutine
    def from_stream(cls, reader: asyncio.StreamReader, fixed_header: MQTTFixedHeader,
                    variable_header: MQTTVariableHeader):
        topics = []
        while True:
            try:
                topic = yield from decode_string(reader)
                qos_byte = yield from read_or_raise(reader, 1)
                qos = bytes_to_int(qos_byte)
                topics.append({'filter': topic, 'qos': qos})
                print(topic)
            except NoDataException:
                break
        return cls(topics)


class SubscribePacket(MQTTPacket):
    VARIABLE_HEADER = PacketIdVariableHeader
    PAYLOAD = SubscribePayload

    def __init__(self, fixed: MQTTFixedHeader=None, variable_header: PacketIdVariableHeader=None, payload=None):
        if fixed is None:
            header = MQTTFixedHeader(PacketType.SUBSCRIBE, 0x00)
        else:
            if fixed.packet_type is not PacketType.SUBSCRIBE:
                raise HBMQTTException("Invalid fixed packet type %s for SubscribePacket init" % fixed.packet_type)
            header = fixed

        super().__init__(header)
        self.variable_header = variable_header
        self.payload = payload
