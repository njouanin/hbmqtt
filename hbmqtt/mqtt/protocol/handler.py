# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import logging

from blinker import Signal

from hbmqtt.mqtt import packet_class
from hbmqtt.mqtt.packet import *
from hbmqtt.mqtt.connack import ConnackPacket
from hbmqtt.mqtt.connect import ConnectPacket
from hbmqtt.mqtt.pingresp import PingRespPacket
from hbmqtt.mqtt.pingreq import PingReqPacket
from hbmqtt.mqtt.publish import PublishPacket
from hbmqtt.mqtt.pubrel import PubrelPacket
from hbmqtt.mqtt.puback import PubackPacket
from hbmqtt.mqtt.pubrec import PubrecPacket
from hbmqtt.mqtt.pubcomp import PubcompPacket
from hbmqtt.mqtt.suback import SubackPacket
from hbmqtt.mqtt.subscribe import SubscribePacket
from hbmqtt.mqtt.unsubscribe import UnsubscribePacket
from hbmqtt.mqtt.unsuback import UnsubackPacket
from hbmqtt.mqtt.disconnect import DisconnectPacket
from hbmqtt.adapters import ReaderAdapter, WriterAdapter
from hbmqtt.session import Session
from hbmqtt.mqtt.constants import *
from hbmqtt.mqtt.protocol.inflight import *
from hbmqtt.plugins.manager import PluginManager

EVENT_MQTT_PACKET_SENT = 'mqtt_packet_sent'
EVENT_MQTT_PACKET_RECEIVED = 'mqtt_packet_received'


class ProtocolHandler:
    """
    Class implementing the MQTT communication protocol using asyncio features
    """

    on_packet_sent = Signal()
    on_packet_received = Signal()

    def __init__(self, reader: ReaderAdapter, writer: WriterAdapter, plugins_manager: PluginManager, loop=None):
        self.logger = logging.getLogger(__name__)
        self.session = None
        self.reader = reader
        self.writer = writer
        self.plugins_manager = plugins_manager
        if loop is None:
            self._loop = asyncio.get_event_loop()
        else:
            self._loop = loop
        self._reader_task = None
        self._writer_task = None
        self._reader_ready = asyncio.Event(loop=self._loop)
        self._writer_ready = asyncio.Event(loop=self._loop)

        self._running = False

        self.outgoing_queue = asyncio.Queue(loop=self._loop)
        self._pubrel_waiters = dict()

    def attach_to_session(self, session: Session):
        self.session = session
        self.session.handler = self

    def detach_from_session(self):
        self.session.handler = None
        self.session = None

    @asyncio.coroutine
    def start(self):
        self._running = True
        self._reader_task = asyncio.Task(self._reader_coro(), loop=self._loop)
        self._writer_task = asyncio.Task(self._writer_coro(), loop=self._loop)
        yield from asyncio.wait(
            [self._reader_ready.wait(), self._writer_ready.wait()], loop=self._loop)
        self.logger.debug("%s Handler tasks started" % self.session.client_id)
        yield from self.retry_deliveries()

    @asyncio.coroutine
    def retry_deliveries(self):
        """
        Handle [MQTT-4.4.0-1] by resending PUBLISH and PUBREL messages for pending out messages
        :return:
        """
        self.logger.debug("Begin messages delivery retries")
        ack_packets = []
        for packet_id in self.session.outgoing_msg:
            message = self.session.outgoing_msg[packet_id]
            if message.is_new() or message.is_published():
                self.logger.debug("Retrying publish message Id=%d", packet_id)
                message.publish_packet.dup_flag = True
                ack = False
                while not ack:
                    yield from self.outgoing_queue.put(message.publish_packet)
                    message.retry_publish()
                    ack = yield from message.wait_acknowledge()
                ack_packets.append(packet_id)
            if message.is_received():
                self.logger.debug("Retrying pubrel message Id=%d", packet_id)
                yield from self.outgoing_queue.put(PubrelPacket.build(packet_id))
                message.sent_pubrel()
        for packet_id in ack_packets:
            del self.session.outgoing_msg[packet_id]
        self.logger.debug("%d messages redelivered" % len(ack_packets))
        self.logger.debug("End messages delivery retries")

    @asyncio.coroutine
    def mqtt_publish(self, topic, message, qos, retain):
        if qos:
            packet_id = self.session.next_packet_id
            if packet_id in self.session.outgoing_msg:
                self.logger.warn("%s A message with the same packet ID is already in flight" % self.session.client_id)
        else:
            packet_id = None
        packet = PublishPacket.build(topic, message, packet_id, False, qos, retain)
        yield from self.outgoing_queue.put(packet)
        if qos != QOS_0:
            inflight_message = OutgoingInFlightMessage(packet, qos, loop=self._loop)
            inflight_message.sent_publish()
            self.session.outgoing_msg[packet_id] = inflight_message
            ack = yield from inflight_message.wait_acknowledge()
            while not ack:
                #Retry publish
                packet = PublishPacket.build(topic, message, packet_id, True, qos, retain)
                self.logger.debug("Retry delivery of packet %s" % repr(packet))
                inflight_message.publish_packet = packet
                yield from self.outgoing_queue.put(packet)
                inflight_message.retry_publish()
                ack = yield from inflight_message.wait_acknowledge()
            del self.session.outgoing_msg[packet_id]

    @asyncio.coroutine
    def stop(self):
        self._running = False
        yield from self.outgoing_queue.put("STOP")
        yield from asyncio.wait([self._writer_task, self._reader_task], loop=self._loop)
        yield from self.writer.close()
        # Stop incoming messages flow waiter
        for packet_id in self.session.incoming_msg:
            self.session.incoming_msg[packet_id].cancel()
        for packet_id in self.session.outgoing_msg:
            self.session.outgoing_msg[packet_id].cancel()

    @asyncio.coroutine
    def _reader_coro(self):
        self.logger.debug("%s Starting reader coro" % self.session.client_id)
        while self._running:
            try:
                self._reader_ready.set()
                keepalive_timeout = self.session.keep_alive
                if keepalive_timeout <= 0:
                    keepalive_timeout = None
                fixed_header = yield from asyncio.wait_for(
                    MQTTFixedHeader.from_stream(self.reader),
                    keepalive_timeout, loop=self._loop)
                if fixed_header:
                    if fixed_header.packet_type == RESERVED_0 or fixed_header.packet_type == RESERVED_15:
                        self.logger.warn("%s Received reserved packet, which is forbidden: closing connection" %
                                         (self.session.client_id))
                        yield from self.handle_connection_closed()
                    else:
                        cls = packet_class(fixed_header)
                        packet = yield from cls.from_stream(self.reader, fixed_header=fixed_header)
                        yield from self.plugins_manager.fire_event(
                            EVENT_MQTT_PACKET_RECEIVED, packet=packet, session=self.session)
                        self._loop.call_soon(self.on_packet_received.send, packet)

                        task = None
                        if packet.fixed_header.packet_type == CONNACK:
                            task = asyncio.Task(self.handle_connack(packet), loop=self._loop)
                        elif packet.fixed_header.packet_type == SUBSCRIBE:
                            task =  asyncio.Task(self.handle_subscribe(packet), loop=self._loop)
                        elif packet.fixed_header.packet_type == UNSUBSCRIBE:
                            task = asyncio.Task(self.handle_unsubscribe(packet), loop=self._loop)
                        elif packet.fixed_header.packet_type == SUBACK:
                            task = asyncio.Task(self.handle_suback(packet), loop=self._loop)
                        elif packet.fixed_header.packet_type == UNSUBACK:
                            task = asyncio.Task(self.handle_unsuback(packet), loop=self._loop)
                        elif packet.fixed_header.packet_type == PUBACK:
                            task = asyncio.Task(self.handle_puback(packet), loop=self._loop)
                        elif packet.fixed_header.packet_type == PUBREC:
                            task = asyncio.Task(self.handle_pubrec(packet), loop=self._loop)
                        elif packet.fixed_header.packet_type == PUBREL:
                            task = asyncio.Task(self.handle_pubrel(packet), loop=self._loop)
                        elif packet.fixed_header.packet_type == PUBCOMP:
                            task = asyncio.Task(self.handle_pubcomp(packet), loop=self._loop)
                        elif packet.fixed_header.packet_type == PINGREQ:
                            task = asyncio.Task(self.handle_pingreq(packet), loop=self._loop)
                        elif packet.fixed_header.packet_type == PINGRESP:
                            task = asyncio.Task(self.handle_pingresp(packet), loop=self._loop)
                        elif packet.fixed_header.packet_type == PUBLISH:
                            task = asyncio.Task(self.handle_publish(packet), loop=self._loop)
                        elif packet.fixed_header.packet_type == DISCONNECT:
                            task = asyncio.Task(self.handle_disconnect(packet), loop=self._loop)
                        elif packet.fixed_header.packet_type == CONNECT:
                            task = asyncio.Task(self.handle_connect(packet), loop=self._loop)
                        else:
                            self.logger.warn("%s Unhandled packet type: %s" %
                                             (self.session.client_id, packet.fixed_header.packet_type))
                        if task:
                            # Wait for message handling ends
                            yield from asyncio.wait([task], loop=self._loop)
                else:
                    self.logger.debug("%s No more data (EOF received), stopping reader coro" % self.session.client_id)
                    yield from self.handle_connection_closed()
                    break
            except asyncio.TimeoutError:
                self.logger.debug("%s Input stream read timeout" % self.session.client_id)
                self.handle_read_timeout()
            except NoDataException as nde:
                self.logger.debug("%s No data available" % self.session.client_id)
            except Exception as e:
                self.logger.warn("%s Unhandled exception in reader coro: %s" % (self.session.client_id, e))
                break
        self.logger.debug("%s Reader coro stopped" % self.session.client_id)

    @asyncio.coroutine
    def _writer_coro(self):
        self.logger.debug("%s Starting writer coro" % self.session.client_id)
        while self._running:
            try:
                self._writer_ready.set()
                keepalive_timeout = self.session.keep_alive
                if keepalive_timeout <= 0:
                    keepalive_timeout = None
                packet = yield from asyncio.wait_for(self.outgoing_queue.get(), keepalive_timeout, loop=self._loop)
                if not isinstance(packet, MQTTPacket):
                    self.logger.debug("%s Writer interruption" % self.session.client_id)
                    break
                yield from packet.to_stream(self.writer)
                yield from self.plugins_manager.fire_event(EVENT_MQTT_PACKET_SENT, packet=packet, session=self.session)
                self._loop.call_soon(self.on_packet_sent.send, packet)
            except asyncio.TimeoutError as ce:
                self.logger.debug("%s Output queue get timeout" % self.session.client_id)
                if self._running:
                    self.handle_write_timeout()
            except ConnectionResetError as cre:
                yield from self.handle_connection_closed()
                break
            except Exception as e:
                self.logger.warn("%sUnhandled exception in writer coro: %s" % (self.session.client_id, e))
                break
        self.logger.debug("%s Writer coro stopping" % self.session.client_id)
        # Flush queue before stopping
        if not self.outgoing_queue.empty():
            while True:
                try:
                    packet = self.outgoing_queue.get_nowait()
                    if not isinstance(packet, MQTTPacket):
                        break
                    yield from packet.to_stream(self.session.writer)
                    self.logger.debug("%s -out-> %s" % (self.session.client_id, repr(packet)))
                    self._loop.call_soon(self.on_packet_sent, packet)
                except asyncio.QueueEmpty:
                    break
                except Exception as e:
                    self.logger.warn("%s Unhandled exception in writer coro: %s" % (self.session.client_id, e))
        self.logger.debug("%s Writer coro stopped" % self.session.client_id)

    @asyncio.coroutine
    def mqtt_deliver_next_message(self):
        packet_id = yield from self.session.delivered_message_queue.get()
        message = self.session.incoming_msg[packet_id]
        if message.qos == QOS_0:
            del self.session.incoming_msg[packet_id]
            self.logger.debug("Discarded incoming message %s" % packet_id)
        return message.publish_packet

    @asyncio.coroutine
    def mqtt_acknowledge_delivery(self, packet_id):
        try:
            message = self.session.incoming_msg[packet_id]
            message.acknowledge_delivery()
            self.logger.debug('Message delivery acknowledged, packed_id=%d' % packet_id)
        except KeyError:
            pass

    def handle_write_timeout(self):
        self.logger.warn('%s write timeout unhandled' % self.session.client_id)

    def handle_read_timeout(self):
        self.logger.warn('%s read timeout unhandled' % self.session.client_id)

    @asyncio.coroutine
    def handle_connack(self, connack: ConnackPacket):
        self.logger.warn('%s CONNACK unhandled' % self.session.client_id)

    @asyncio.coroutine
    def handle_connect(self, connect: ConnectPacket):
        self.logger.warn('%s CONNECT unhandled' % self.session.client_id)

    @asyncio.coroutine
    def handle_subscribe(self, subscribe: SubscribePacket):
        self.logger.warn('%s SUBSCRIBE unhandled' % self.session.client_id)

    @asyncio.coroutine
    def handle_unsubscribe(self, subscribe: UnsubscribePacket):
        self.logger.warn('%s UNSUBSCRIBE unhandled' % self.session.client_id)

    @asyncio.coroutine
    def handle_suback(self, suback: SubackPacket):
        self.logger.warn('%s SUBACK unhandled' % self.session.client_id)

    @asyncio.coroutine
    def handle_unsuback(self, unsuback: UnsubackPacket):
        self.logger.warn('%s UNSUBACK unhandled' % self.session.client_id)

    @asyncio.coroutine
    def handle_pingresp(self, pingresp: PingRespPacket):
        self.logger.warn('%s PINGRESP unhandled' % self.session.client_id)

    @asyncio.coroutine
    def handle_pingreq(self, pingreq: PingReqPacket):
        self.logger.warn('%s PINGREQ unhandled' % self.session.client_id)

    @asyncio.coroutine
    def handle_disconnect(self, disconnect: DisconnectPacket):
        self.logger.warn('%s DISCONNECT unhandled' % self.session.client_id)

    @asyncio.coroutine
    def handle_connection_closed(self):
        self.logger.warn('%s Connection closed unhandled' % self.session.client_id)

    @asyncio.coroutine
    def handle_puback(self, puback: PubackPacket):
        packet_id = puback.variable_header.packet_id
        try:
            inflight_message = self.session.outgoing_msg[packet_id]
            inflight_message.received_puback()
        except KeyError as ke:
            self.logger.warn("%s Received PUBACK for unknown pending subscription with Id: %s" %
                             (self.session.client_id, packet_id))

    @asyncio.coroutine
    def handle_pubrec(self, pubrec: PubrecPacket):
        packet_id = pubrec.variable_header.packet_id
        try:
            inflight_message = self.session.outgoing_msg[packet_id]
            inflight_message.received_pubrec()
            yield from self.outgoing_queue.put(PubrelPacket.build(packet_id))
            inflight_message.sent_pubrel()
        except KeyError as ke:
            self.logger.warn("Received PUBREC for unknown pending subscription with Id: %s" % packet_id)

    @asyncio.coroutine
    def handle_pubcomp(self, pubcomp: PubcompPacket):
        packet_id = pubcomp.variable_header.packet_id
        try:
            inflight_message = self.session.outgoing_msg[packet_id]
            inflight_message.received_pubcomp()
        except KeyError as ke:
            self.logger.warn("Received PUBCOMP for unknown pending subscription with Id: %s" % packet_id)

    @asyncio.coroutine
    def handle_pubrel(self, pubrel: PubrecPacket):
        packet_id = pubrel.variable_header.packet_id
        try:
            inflight_message = self.session.incoming_msg[packet_id]
            inflight_message.received_pubrel()
        except KeyError as ke:
            self.logger.warn("Received PUBREL for unknown pending subscription with Id: %s" % packet_id)

    @asyncio.coroutine
    def handle_publish(self, publish_packet: PublishPacket):
        incoming_message = None
        packet_id = publish_packet.variable_header.packet_id
        qos = publish_packet.qos

        if qos == 0:
            if publish_packet.dup_flag:
                self.logger.warn("[MQTT-3.3.1-2] DUP flag must set to 0 for QOS 0 message. Message ignored: %s" %
                                 repr(publish_packet))
            else:
                # Assign packet_id as it's needed internally
                packet_id = self.session.next_packet_id
                publish_packet.variable_header.packet_id = packet_id
                incoming_message = IncomingInFlightMessage(publish_packet,
                                                           qos,
                                                           self.session.publish_retry_delay,
                                                           self._loop)
                incoming_message.received_publish()
                self.session.incoming_msg[packet_id] = incoming_message
                yield from self.session.delivered_message_queue.put(packet_id)
        else:
            # Check if publish is a retry
            if packet_id in self.session.incoming_msg:
                incoming_message = self.session.incoming_msg[packet_id]
            else:
                incoming_message = IncomingInFlightMessage(publish_packet,
                                                           qos,
                                                           self.session.publish_retry_delay,
                                                           self._loop)
                self.session.incoming_msg[packet_id] = incoming_message
                incoming_message.publish()

            if qos == 1:
                # Initiate delivery
                yield from self.session.delivered_message_queue.put(packet_id)
                ack = yield from incoming_message.wait_acknowledge()
                if ack:
                    # Send PUBACK
                    puback = PubackPacket.build(packet_id)
                    yield from self.outgoing_queue.put(puback)
                    #Discard message
                    del self.session.incoming_msg[packet_id]
                    self.logger.debug("Discarded incoming message %d" % packet_id)
                else:
                    raise HBMQTTException("Something wrong, ack is False")
            if qos == 2:
                # Send PUBREC
                pubrec = PubrecPacket.build(packet_id)
                yield from self.outgoing_queue.put(pubrec)
                incoming_message.sent_pubrec()
                # Wait for pubrel
                ack = yield from incoming_message.wait_pubrel()
                if ack:
                    # Initiate delivery
                    yield from self.session.delivered_message_queue.put(packet_id)
                else:
                    raise HBMQTTException("Something wrong, ack is False")
                ack = yield from incoming_message.wait_acknowledge()
                if ack:
                    # Send PUBCOMP
                    pubcomp = PubcompPacket.build(packet_id)
                    yield from self.outgoing_queue.put(pubcomp)
                    incoming_message.sent_pubcomp()
                    #Discard message
                    del self.session.incoming_msg[packet_id]
                    self.logger.debug("Discarded incoming message %d" % packet_id)
                else:
                    raise HBMQTTException("Something wrong, ack is False")
