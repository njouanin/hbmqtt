# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import asyncio
from asyncio import futures
from hbmqtt.mqtt.protocol.handler import ProtocolHandler
from hbmqtt.mqtt.connect import ConnectPacket
from hbmqtt.mqtt.pingreq import PingReqPacket
from hbmqtt.mqtt.pingresp import PingRespPacket
from hbmqtt.mqtt.subscribe import SubscribePacket
from hbmqtt.mqtt.suback import SubackPacket
from hbmqtt.mqtt.unsubscribe import UnsubscribePacket
from hbmqtt.mqtt.unsuback import UnsubackPacket
from hbmqtt.utils import format_client_message
from hbmqtt.adapters import WriterAdapter, ReaderAdapter
from hbmqtt.plugins.manager import PluginManager


class BrokerProtocolHandler(ProtocolHandler):
    def __init__(self, reader: ReaderAdapter, writer: WriterAdapter, plugins_manager: PluginManager, loop=None):
        super().__init__(reader, writer, plugins_manager, loop)
        self._disconnect_waiter = None
        self._pending_subscriptions = asyncio.Queue()
        self._pending_unsubscriptions = asyncio.Queue()

    @asyncio.coroutine
    def start(self):
        yield from super().start()
        if self._disconnect_waiter is None:
            self._disconnect_waiter = futures.Future(loop=self._loop)

    @asyncio.coroutine
    def stop(self):
        yield from super().stop()
        if self._disconnect_waiter is not None and not self._disconnect_waiter.done():
            self._disconnect_waiter.set_result(None)

    @asyncio.coroutine
    def wait_disconnect(self):
        return (yield from self._disconnect_waiter)

    def handle_write_timeout(self):
        pass

    def handle_read_timeout(self):
        if self._disconnect_waiter is not None and not self._disconnect_waiter.done():
            self._disconnect_waiter.set_result(None)

    @asyncio.coroutine
    def handle_disconnect(self, disconnect):
        if self._disconnect_waiter and not self._disconnect_waiter.done():
            self._disconnect_waiter.set_result(disconnect)

    @asyncio.coroutine
    def handle_connection_closed(self):
        yield from self.handle_disconnect(None)

    @asyncio.coroutine
    def handle_connect(self, connect: ConnectPacket):
        # Broker handler shouldn't received CONNECT message during messages handling
        # as CONNECT messages are managed by the broker on client connection
        self.logger.error('%s [MQTT-3.1.0-2] %s : CONNECT message received during messages handling' %
                          (self.session.client_id, format_client_message(self.session)))
        if self._disconnect_waiter is not None and not self._disconnect_waiter.done():
            self._disconnect_waiter.set_result(None)

    @asyncio.coroutine
    def handle_pingreq(self, pingreq: PingReqPacket):
        yield from self.outgoing_queue.put(PingRespPacket.build())

    @asyncio.coroutine
    def handle_subscribe(self, subscribe: SubscribePacket):
        subscription = {'packet_id': subscribe.variable_header.packet_id, 'topics': subscribe.payload.topics}
        yield from self._pending_subscriptions.put(subscription)

    @asyncio.coroutine
    def handle_unsubscribe(self, unsubscribe: UnsubscribePacket):
        unsubscription = {'packet_id': unsubscribe.variable_header.packet_id, 'topics': unsubscribe.payload.topics}
        yield from self._pending_unsubscriptions.put(unsubscription)

    @asyncio.coroutine
    def get_next_pending_subscription(self):
        subscription = yield from self._pending_subscriptions.get()
        return subscription

    @asyncio.coroutine
    def get_next_pending_unsubscription(self):
        unsubscription = yield from self._pending_unsubscriptions.get()
        return unsubscription

    @asyncio.coroutine
    def mqtt_acknowledge_subscription(self, packet_id, return_codes):
        suback = SubackPacket.build(packet_id, return_codes)
        yield from self.outgoing_queue.put(suback)

    @asyncio.coroutine
    def mqtt_acknowledge_unsubscription(self, packet_id):
        unsuback = UnsubackPacket.build(packet_id)
        yield from self.outgoing_queue.put(unsuback)