# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import logging
import asyncio
from asyncio import futures
from hbmqtt.mqtt.protocol.handler import ProtocolHandler
from hbmqtt.mqtt.packet import MQTTFixedHeader
from hbmqtt.mqtt.packet import PacketType
from hbmqtt.mqtt.connect import ConnectVariableHeader, ConnectPacket, ConnectPayload
from hbmqtt.mqtt.connack import ConnackPacket
from hbmqtt.mqtt.disconnect import DisconnectPacket
from hbmqtt.mqtt.pingreq import PingReqPacket
from hbmqtt.mqtt.pingresp import PingRespPacket
from hbmqtt.mqtt.subscribe import SubscribePacket
from hbmqtt.mqtt.suback import SubackPacket
from hbmqtt.mqtt.unsubscribe import UnsubscribePacket
from hbmqtt.mqtt.unsuback import UnsubackPacket
from hbmqtt.session import Session

class BrokerProtocolHandler(ProtocolHandler):
    def __init__(self, session: Session, loop=None):
        super().__init__(session, loop)
        self._disconnect_waiter = None

    @asyncio.coroutine
    def start(self):
        yield from super().start()

    @asyncio.coroutine
    def stop(self):
        yield from super().stop()

    @asyncio.coroutine
    def wait_disconnect(self):
        if self._disconnect_waiter is None:
            self._disconnect_waiter = futures.Future(loop=self._loop)
        yield from self._disconnect_waiter

    @asyncio.coroutine
    def handle_disconnect(self, disconnect: DisconnectPacket):
        if self._disconnect_waiter is not None:
            self._disconnect_waiter.set_result(disconnect)
