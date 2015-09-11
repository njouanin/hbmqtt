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
from hbmqtt.session import Session, OutgoingApplicationMessage, IncomingApplicationMessage
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

    def __init__(self, session: Session, plugins_manager: PluginManager, loop=None):
        log = logging.getLogger(__name__)
        self.logger = logging.LoggerAdapter(log, {'client_id': session.client_id})
        self.session = session
        self.reader = session.reader
        self.writer = session.writer
        self.plugins_manager = plugins_manager
        if loop is None:
            self._loop = asyncio.get_event_loop()
        else:
            self._loop = loop
        self._reader_task = None
        self._writer_task = None
        self._reader_ready = asyncio.Event(loop=self._loop)
        self._writer_ready = asyncio.Event(loop=self._loop)
        self._reader_stopped = asyncio.Event(loop=self._loop)
        self._writer_stopped = asyncio.Event(loop=self._loop)

        self.outgoing_queue = asyncio.Queue(loop=self._loop)
        self._puback_waiters = dict()
        self._pubrec_waiters = dict()
        self._pubrel_waiters = dict()
        self._pubcomp_waiters = dict()

    @asyncio.coroutine
    def start(self):
        self._reader_task = asyncio.Task(self._reader_loop(), loop=self._loop)
        self._writer_task = asyncio.Task(self._writer_loop(), loop=self._loop)
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
            if message.is_acknowledged():
                ack_packets.append(packet_id)
            else:
                if not message.pubrec_packet:
                    self.logger.debug("Retrying publish message Id=%d acknowledgment", packet_id)
                    message.publish_packet = PublishPacket.build(
                        message.topic,
                        message.data,
                        message.packet_id,
                        True,
                        message.qos,
                        message.retain)
                    yield from self.outgoing_queue.put(message.publish_packet)
                yield from self._handle_message_flow(message)
        for packet_id in ack_packets:
            del self.session.outgoing_msg[packet_id]
        self.logger.debug("%d messages redelivered" % len(ack_packets))
        self.logger.debug("End messages delivery retries")

    @asyncio.coroutine
    def mqtt_publish(self, topic, data, qos, retain, ack_timeout=None):
        if qos in (QOS_1, QOS_2):
            packet_id = self.session.next_packet_id
            if packet_id in self.session.outgoing_msg:
                raise HBMQTTException("A message with the same packet ID '%d' is already in flight" % packet_id)
        else:
            packet_id = None

        message = OutgoingApplicationMessage(packet_id, topic, qos, data, retain)
        if qos in (QOS_1, QOS_2):
            # wait for message acknowledge management to complete
            yield from asyncio.wait_for(self._handle_message_flow(message), 10, loop=self._loop)
        return message

    @asyncio.coroutine
    def _handle_message_flow(self, app_message):
        """
        Handle protocol flow for incoming and outgoing messages, depending on service level and according to MQTT
        spec. paragraph 4.3-Quality of Service levels and protocol flows
        :param app_message: PublishMessage to handle
        :return: nothing.
        """
        if app_message.qos == QOS_0:
            # QOS 0 packet don't need acknowledgment
            return
        if app_message.qos == QOS_1:
            yield from self._handle_qos1_message_flow(app_message)
        elif app_message.qos == QOS_2:
            if not app_message.pubrec_packet:
                if app_message.packet_id in self._pubrec_waiters:
                    # PUBREC waiter already exists for this packet ID
                    message = "Can't add PUBREC waiter, a waiter already exists for message Id '%s'" \
                              % app_message.packet_id
                    self.logger.warning(message)
                    raise HBMQTTException(message)
                # Wait for PUBREC
                waiter = asyncio.Future(loop=self._loop)
                self._pubrec_waiters[app_message.packet_id] = waiter
                yield from waiter
                del self._pubrec_waiters[app_message.packet_id]
                app_message.pubrec_packet = waiter.result()

            if not app_message.pubrel_packet:
                # Send pubrel
                app_message.pubrel_packet = PubrelPacket.build(app_message.packet_id)
                yield from self.outgoing_queue.put(app_message.pubrel_packet)

            if not app_message.pubcomp_packet:
                # Wait for PUBCOMP
                waiter = asyncio.Future(loop=self._loop)
                self._pubcomp_waiters[app_message.packet_id] = waiter
                yield from waiter
                del self._pubcomp_waiters[app_message.packet_id]
                app_message.pubcomp_packet = waiter.result()
        else:
            raise HBMQTTException("Unexcepted QOS value '%d" % str(app_message.qos))
        # Discard acknowledged message
        if app_message.is_acknowledged():
            del self.session.outgoing_msg[app_message.packet_id]

    @asyncio.coroutine
    def _handle_qos1_message_flow(self, app_message):
        """
        Handle QOS_1 application message acknowledgment
        For incoming messages, this method stores the messages and reply with PUBACK
        For outgoing messages, this methods sends PUBLISH and waits for the corresponding PUBACK
        :param app_message:
        :return:
        """
        assert app_message.qos == QOS_1
        if isinstance(app_message, OutgoingApplicationMessage):
            if app_message.is_acknowledged():
                raise HBMQTTException("Message '%d' already sent and acknowledged" % app_message.packet_id)
            # Store message
            if app_message.publish_packet is not None:
                # This is a retry flow, no need to store just check the message exists in session
                if app_message.packet_id not in self.session.outgoing_msg:
                    raise HBMQTTException("Unknown inflight message '%d' in session" % app_message.packet_id)
                app_message.build_publish_packet(dup=True)
            else:
                # Store QOS_1 and QOS_2 messages in session
                self.session.outgoing_msg[app_message.packet_id] = app_message
                app_message.build_publish_packet(dup=False)

            # Send PUBLISH packet
            yield from self.outgoing_queue.put(message.publish_packet)

            # Wait for puback
            waiter = asyncio.Future(loop=self._loop)
            self._puback_waiters[app_message.packet_id] = waiter
            yield from waiter
            del self._puback_waiters[app_message.packet_id]
            app_message.puback_packet = waiter.result()
        elif isinstance(app_message, IncomingApplicationMessage);
            pass

    @asyncio.coroutine
    def stop(self):
        # Stop incoming messages flow waiter
        for packet_id in self.session.incoming_msg:
            self.session.incoming_msg[packet_id].cancel()
#        for packet_id in self.session.outgoing_msg:
#            self.session.outgoing_msg[packet_id].cancel()
        self._reader_task.cancel()
        self._writer_task.cancel()
        self.logger.debug("waiting for loops to be stopped")
        yield from asyncio.wait(
            [self._reader_stopped.wait(), self._writer_stopped.wait()], loop=self._loop)
        self.logger.debug("closing writer")
        yield from self.writer.close()

    @asyncio.coroutine
    def _reader_loop(self):
        self.logger.debug("%s Starting reader coro" % self.session.client_id)
        while True:
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
                        self.logger.warning("%s Received reserved packet, which is forbidden: closing connection" %
                                         (self.session.client_id))
                        yield from self.handle_connection_closed()
                    else:
                        cls = packet_class(fixed_header)
                        packet = yield from cls.from_stream(self.reader, fixed_header=fixed_header)
                        yield from self.plugins_manager.fire_event(
                            EVENT_MQTT_PACKET_RECEIVED, packet=packet, session=self.session)
                        self._loop.call_soon(self.on_packet_received.send, packet)

                        if packet.fixed_header.packet_type == CONNACK:
                            yield from self.handle_connack(packet)
                        elif packet.fixed_header.packet_type == SUBSCRIBE:
                            yield from self.handle_subscribe(packet)
                        elif packet.fixed_header.packet_type == UNSUBSCRIBE:
                            yield from self.handle_unsubscribe(packet)
                        elif packet.fixed_header.packet_type == SUBACK:
                            yield from self.handle_suback(packet)
                        elif packet.fixed_header.packet_type == UNSUBACK:
                            yield from self.handle_unsuback(packet)
                        elif packet.fixed_header.packet_type == PUBACK:
                            yield from self.handle_puback(packet)
                        elif packet.fixed_header.packet_type == PUBREC:
                            yield from self.handle_pubrec(packet)
                        elif packet.fixed_header.packet_type == PUBREL:
                            yield from self.handle_pubrel(packet)
                        elif packet.fixed_header.packet_type == PUBCOMP:
                            yield from self.handle_pubcomp(packet)
                        elif packet.fixed_header.packet_type == PINGREQ:
                            yield from self.handle_pingreq(packet)
                        elif packet.fixed_header.packet_type == PINGRESP:
                            yield from self.handle_pingresp(packet)
                        elif packet.fixed_header.packet_type == PUBLISH:
                            yield from self.handle_publish(packet)
                        elif packet.fixed_header.packet_type == DISCONNECT:
                            yield from self.handle_disconnect(packet)
                        elif packet.fixed_header.packet_type == CONNECT:
                            self.handle_connect(packet)
                        else:
                            self.logger.warning("%s Unhandled packet type: %s" %
                                             (self.session.client_id, packet.fixed_header.packet_type))
                else:
                    self.logger.debug("%s No more data (EOF received), stopping reader coro" % self.session.client_id)
                    break
            except asyncio.CancelledError:
                self.logger.debug("Task cancelled, reader loop ending")
                break
            except asyncio.TimeoutError:
                self.logger.debug("%s Input stream read timeout" % self.session.client_id)
                self.handle_read_timeout()
            except NoDataException as nde:
                self.logger.debug("%s No data available" % self.session.client_id)
            except Exception as e:
                self.logger.warning("%s Unhandled exception in reader coro: %s" % (self.session.client_id, e))
                break
        yield from self.handle_connection_closed()
        self._reader_stopped.set()
        self.logger.debug("%s Reader coro stopped" % self.session.client_id)

    @asyncio.coroutine
    def _writer_loop(self):
        self.logger.debug("%s Starting writer coro" % self.session.client_id)
        while True:
            try:
                self._writer_ready.set()
                keepalive_timeout = self.session.keep_alive
                if keepalive_timeout <= 0:
                    keepalive_timeout = None
                packet = yield from asyncio.wait_for(self.outgoing_queue.get(), keepalive_timeout, loop=self._loop)
                yield from packet.to_stream(self.writer)
                yield from self.plugins_manager.fire_event(EVENT_MQTT_PACKET_SENT, packet=packet, session=self.session)
                self._loop.call_soon(self.on_packet_sent.send, packet)
            except asyncio.CancelledError:
                self.logger.debug("Task cancelled, writer loop ending")
                break
            except asyncio.TimeoutError as ce:
                # self.logger.debug("%s Output queue get timeout" % self.session.client_id)
                self.handle_write_timeout()
            except ConnectionResetError as cre:
                yield from self.handle_connection_closed()
                break
            except Exception as e:
                self.logger.warning("%sUnhandled exception in writer coro: %s" % (self.session.client_id, e))
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
                    yield from self.plugins_manager.fire_event(EVENT_MQTT_PACKET_SENT, packet=packet, session=self.session)
                    self._loop.call_soon(self.on_packet_sent.send, packet)
                except asyncio.QueueEmpty:
                    break
                except Exception as e:
                    self.logger.warning("%s Unhandled exception in writer coro: %s" % (self.session.client_id, e))
        self._writer_stopped.set()
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
        self.logger.warning('%s write timeout unhandled' % self.session.client_id)

    def handle_read_timeout(self):
        self.logger.warning('%s read timeout unhandled' % self.session.client_id)

    @asyncio.coroutine
    def handle_connack(self, connack: ConnackPacket):
        self.logger.warning('%s CONNACK unhandled' % self.session.client_id)

    @asyncio.coroutine
    def handle_connect(self, connect: ConnectPacket):
        self.logger.warning('%s CONNECT unhandled' % self.session.client_id)

    @asyncio.coroutine
    def handle_subscribe(self, subscribe: SubscribePacket):
        self.logger.warning('%s SUBSCRIBE unhandled' % self.session.client_id)

    @asyncio.coroutine
    def handle_unsubscribe(self, subscribe: UnsubscribePacket):
        self.logger.warning('%s UNSUBSCRIBE unhandled' % self.session.client_id)

    @asyncio.coroutine
    def handle_suback(self, suback: SubackPacket):
        self.logger.warning('%s SUBACK unhandled' % self.session.client_id)

    @asyncio.coroutine
    def handle_unsuback(self, unsuback: UnsubackPacket):
        self.logger.warning('%s UNSUBACK unhandled' % self.session.client_id)

    @asyncio.coroutine
    def handle_pingresp(self, pingresp: PingRespPacket):
        self.logger.warning('%s PINGRESP unhandled' % self.session.client_id)

    @asyncio.coroutine
    def handle_pingreq(self, pingreq: PingReqPacket):
        self.logger.warning('%s PINGREQ unhandled' % self.session.client_id)

    @asyncio.coroutine
    def handle_disconnect(self, disconnect: DisconnectPacket):
        self.logger.warning('%s DISCONNECT unhandled' % self.session.client_id)

    @asyncio.coroutine
    def handle_connection_closed(self):
        self.logger.warning('%s Connection closed unhandled' % self.session.client_id)

    @asyncio.coroutine
    def handle_puback(self, puback: PubackPacket):
        packet_id = puback.variable_header.packet_id
        try:
            waiter = self._puback_waiters[packet_id]
            waiter.set_result(puback)
        except KeyError:
            self.logger.warning("%s Received PUBACK for unknown pending message Id: '%s'" %
                             (self.session.client_id, packet_id))

    @asyncio.coroutine
    def handle_pubrec(self, pubrec: PubrecPacket):
        packet_id = pubrec.packet_id
        try:
            waiter = self._pubrec_waiters[packet_id]
            waiter.set_result(pubrec)
        except KeyError:
            self.logger.warning("Received PUBREC for unknown pending message with Id: %s" % packet_id)

    @asyncio.coroutine
    def handle_pubcomp(self, pubcomp: PubcompPacket):
        packet_id = pubcomp.packet_id
        try:
            waiter = self._pubcomp_waiters[packet_id]
            waiter.set_result(pubcomp)
        except KeyError:
            self.logger.warning("Received PUBCOMP for unknown pending message with Id: %s" % packet_id)

    @asyncio.coroutine
    def handle_pubrel(self, pubrel: PubrecPacket):
        packet_id = pubrel.packet_id
        try:
            inflight_message = self.session.incoming_msg[packet_id]
            inflight_message.received_pubrel()
        except KeyError as ke:
            self.logger.warning("Received PUBREL for unknown pending subscription with Id: %s" % packet_id)

    @asyncio.coroutine
    def handle_publish(self, publish_packet: PublishPacket):
        incoming_message = None
        packet_id = publish_packet.variable_header.packet_id
        qos = publish_packet.qos

        if qos == 0:
            if publish_packet.dup_flag:
                self.logger.warning("[MQTT-3.3.1-2] DUP flag must set to 0 for QOS 0 message. Message ignored: %s" %
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
                    # Discard message
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
                    # Discard message
                    del self.session.incoming_msg[packet_id]
                    self.logger.debug("Discarded incoming message %d" % packet_id)
                else:
                    raise HBMQTTException("Something wrong, ack is False")
