# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import logging
import asyncio
from datetime import datetime
from asyncio import futures
from hbmqtt.mqtt.packet import MQTTFixedHeader, MQTTPacket
from hbmqtt.mqtt import packet_class
from hbmqtt.errors import NoDataException, HBMQTTException
from hbmqtt.mqtt.packet import PacketType
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
from hbmqtt.session import Session
from hbmqtt.specs import *
from transitions import Machine, MachineError


class InFlightMessage:
    states = ['new', 'published', 'acknowledged', 'received', 'released', 'completed']

    def __init__(self, packet, qos, ack_timeout=0, loop=None):
        if loop is None:
            self._loop = asyncio.get_event_loop()
        else:
            self._loop = loop
        self.publish_packet = packet
        self.qos = qos
        self.publish_ts = None
        self.puback_ts = None
        self.pubrec_ts = None
        self.pubrel_ts = None
        self.pubcomp_ts = None
        self.nb_retries = 0
        self._ack_waiter = asyncio.Future(loop=self._loop)
        self._ack_timeout = ack_timeout
        self._ack_timeout_handle = None
        self._init_states()

    def _init_states(self):
        self.machine = Machine(model=self, states=InFlightMessage.states, initial='new')
        self.machine.add_transition(trigger='publish', source='new', dest='published')
        self.machine.add_transition(trigger='publish', source='published', dest='published')
        self.machine.add_transition(trigger='publish', source='received', dest='published')
        self.machine.add_transition(trigger='publish', source='released', dest='published')
        if self.qos == 0x01:
            self.machine.add_transition(trigger='acknowledge', source='published', dest='acknowledged')
        if self.qos == 0x02:
            self.machine.add_transition(trigger='receive', source='published', dest='received')
            self.machine.add_transition(trigger='release', source='received', dest='released')
            self.machine.add_transition(trigger='complete', source='released', dest='completed')

    @asyncio.coroutine
    def wait_acknowledge(self):
        return (yield from self._ack_waiter)

    def received_puback(self):
        try:
            self.acknowledge()
            self.puback_ts = datetime.now()
            self.cancel_ack_timeout()
            self._ack_waiter.set_result(True)
        except MachineError:
            raise HBMQTTException(
                'Invalid call to method received_puback on inflight messages with QOS=%d, state=%s' %
                (self.qos, self.state))

    def received_pubrec(self):
        try:
            self.receive()
            self.pubrec_ts = datetime.now()
            self.publish_packet = None  # Discard message
            self.reset_ack_timeout()
        except MachineError:
            raise HBMQTTException(
                'Invalid call to method received_pubrec on inflight messages with QOS=%d, state=%s' %
                (self.qos, self.state))

    def received_pubcomp(self):
        try:
            self.complete()
            self.pubcomp_ts = datetime.now()
            self.cancel_ack_timeout()
            self._ack_waiter.set_result(True)
        except MachineError:
            raise HBMQTTException(
                'Invalid call to method received_pubcomp on inflight messages with QOS=%d, state=%s' %
                (self.qos, self.state))

    def sent_pubrel(self):
        try:
            self.release()
            self.pubrel_ts = datetime.now()
        except MachineError:
            raise HBMQTTException(
                'Invalid call to method sent_pubrel on inflight messages with QOS=%d, state=%s' %
                (self.qos, self.state))

    def retry_publish(self):
        try:
            self.publish()
            self.nb_retries += 1
            self.publish_ts = datetime.now()
            self.start_ack_timeout()
        except MachineError:
            raise HBMQTTException(
                'Invalid call to method retry_publish on inflight messages with QOS=%d, state=%s' %
                (self.qos, self.state))

    def sent_publish(self):
        try:
            self.publish()
            self.publish_ts = datetime.now()
            self.start_ack_timeout()
        except MachineError:
            raise HBMQTTException(
                'Invalid call to method sent_publish on inflight messages with QOS=%d, state=%s' %
                (self.qos, self.state))

    def start_ack_timeout(self):
        def cb_timeout():
            self._ack_waiter.set_result(False)
        if self._ack_timeout:
            self._ack_timeout_handle = self._loop.call_later(self._ack_timeout, cb_timeout)

    def cancel_ack_timeout(self):
        if self._ack_timeout_handle:
            self._ack_timeout_handle.cancel()

    def reset_ack_timeout(self):
        self.cancel_ack_timeout()
        self.start_ack_timeout()


class ProtocolHandler:
    """
    Class implementing the MQTT communication protocol using asyncio features
    """

    def __init__(self, loop=None):
        self.logger = logging.getLogger(__name__)
        self.session = None
        if loop is None:
            self._loop = asyncio.get_event_loop()
        else:
            self._loop = loop
        self._reader_task = None
        self._writer_task = None
        self._reader_ready = asyncio.Event(loop=self._loop)
        self._writer_ready = asyncio.Event(loop=self._loop)

        self._running = False

        self.outgoing_queue = asyncio.Queue()
        self._pubrel_waiters = dict()
        self.delivered_message = asyncio.Queue()

    def attach_to_session(self, session: Session):
        self.session = session
        self.session.handler = self
        extra_info = self.session.writer.get_extra_info('sockname')
        self.session.local_address = extra_info[0]
        self.session.local_port = extra_info[1]

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
        for packet_id in self.session.inflight_out:
            message = self.session.inflight_out[packet_id]
            if message.is_new():
                self.logger.debug("Retrying publish message Id=%d", packet_id)
                message.publish_packet.dup_flag = True
                ack = False
                while not ack:
                    yield from self.outgoing_queue.put(message.publish_packet)
                    message.retry_publish()
                    ack = yield from message.wait_acknowledge()
                del self.session.inflight_out[packet_id]
            if message.is_received():
                self.logger.debug("Retrying pubrel message Id=%d", packet_id)
                yield from self.outgoing_queue.put(PubrelPacket.build(packet_id))
                message.sent_pubrel()
        self.logger.debug("End messages delivery retries")

    @asyncio.coroutine
    def mqtt_publish(self, topic, message, qos, retain):
        packet_id = self.session.next_packet_id
        if packet_id in self.session.inflight_out:
            self.logger.warn("%s A message with the same packet ID is already in flight" % self.session.client_id)
        packet = PublishPacket.build(topic, message, packet_id, False, qos, retain)
        yield from self.outgoing_queue.put(packet)
        if qos != QOS_0:
            inflight_message = InFlightMessage(packet, qos, loop=self._loop)
            inflight_message.sent_publish()
            self.session.inflight_out[packet_id] = inflight_message
            ack = yield from inflight_message.wait_acknowledge()
            while not ack:
                #Retry publish
                packet = PublishPacket.build(topic, message, packet_id, True, qos, retain)
                inflight_message.publish_packet = packet
                yield from self.outgoing_queue.put(packet)
                inflight_message.retry_publish()
                ack = yield from inflight_message.wait_acknowledge()
            del self.session.inflight_out[packet_id]

    @asyncio.coroutine
    def stop(self):
        self._running = False
        self.session.reader.feed_eof()
        yield from self.outgoing_queue.put("STOP")
        yield from asyncio.wait([self._writer_task, self._reader_task], loop=self._loop)

    @asyncio.coroutine
    def _reader_coro(self):
        self.logger.debug("%s Starting reader coro" % self.session.client_id)
        while self._running:
            try:
                self._reader_ready.set()
                keepalive_timeout = self.session.keep_alive
                if keepalive_timeout <= 0:
                    keepalive_timeout = None
                fixed_header = yield from asyncio.wait_for(MQTTFixedHeader.from_stream(self.session.reader), keepalive_timeout)
                if fixed_header:
                    cls = packet_class(fixed_header)
                    packet = yield from cls.from_stream(self.session.reader, fixed_header=fixed_header)
                    self.logger.debug("%s <-in-- %s" % (self.session.client_id, repr(packet)))

                    if packet.fixed_header.packet_type == PacketType.CONNACK:
                        asyncio.Task(self.handle_connack(packet))
                    elif packet.fixed_header.packet_type == PacketType.SUBSCRIBE:
                        asyncio.Task(self.handle_subscribe(packet))
                    elif packet.fixed_header.packet_type == PacketType.UNSUBSCRIBE:
                        asyncio.Task(self.handle_unsubscribe(packet))
                    elif packet.fixed_header.packet_type == PacketType.SUBACK:
                        asyncio.Task(self.handle_suback(packet))
                    elif packet.fixed_header.packet_type == PacketType.UNSUBACK:
                        asyncio.Task(self.handle_unsuback(packet))
                    elif packet.fixed_header.packet_type == PacketType.PUBACK:
                        asyncio.Task(self.handle_puback(packet))
                    elif packet.fixed_header.packet_type == PacketType.PUBREC:
                        asyncio.Task(self.handle_pubrec(packet))
                    elif packet.fixed_header.packet_type == PacketType.PUBREL:
                        asyncio.Task(self.handle_pubrel(packet))
                    elif packet.fixed_header.packet_type == PacketType.PUBCOMP:
                        asyncio.Task(self.handle_pubcomp(packet))
                    elif packet.fixed_header.packet_type == PacketType.PINGREQ:
                        asyncio.Task(self.handle_pingreq(packet))
                    elif packet.fixed_header.packet_type == PacketType.PINGRESP:
                        asyncio.Task(self.handle_pingresp(packet))
                    elif packet.fixed_header.packet_type == PacketType.PUBLISH:
                        asyncio.Task(self.handle_publish(packet))
                    elif packet.fixed_header.packet_type == PacketType.DISCONNECT:
                        asyncio.Task(self.handle_disconnect(packet))
                    elif packet.fixed_header.packet_type == PacketType.CONNECT:
                        asyncio.Task(self.handle_connect(packet))
                    else:
                        self.logger.warn("%s Unhandled packet type: %s" %
                                         (self.session.client_id, packet.fixed_header.packet_type))
                else:
                    self.logger.debug("%s No more data, stopping reader coro" % self.session.client_id)
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
                packet = yield from asyncio.wait_for(self.outgoing_queue.get(), keepalive_timeout)
                if not isinstance(packet, MQTTPacket):
                    self.logger.debug("%s Writer interruption" % self.session.client_id)
                    break
                yield from packet.to_stream(self.session.writer)
                self.logger.debug("%s -out-> %s" % (self.session.client_id, repr(packet)))
                yield from self.session.writer.drain()
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
                except asyncio.QueueEmpty:
                    break
                except Exception as e:
                    self.logger.warn("%s Unhandled exception in writer coro: %s" % (self.session.client_id, e))
        self.logger.debug("%s Writer coro stopped" % self.session.client_id)

    @asyncio.coroutine
    def mqtt_deliver_next_message(self):
        inflight_message = yield from self.delivered_message.get()
        return inflight_message

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
            inflight_message = self.session.inflight_out[packet_id]
            inflight_message.received_puback()
        except KeyError as ke:
            self.logger.warn("%s Received PUBACK for unknown pending subscription with Id: %s" %
                             (self.session.client_id, packet_id))

    @asyncio.coroutine
    def handle_pubrec(self, pubrec: PubrecPacket):
        packet_id = pubrec.variable_header.packet_id
        try:
            inflight_message = self.session.inflight_out[packet_id]
            inflight_message.received_pubrec()
            yield from self.outgoing_queue.put(PubrelPacket.build(packet_id))
            inflight_message.sent_pubrel()
        except KeyError as ke:
            self.logger.warn("Received PUBREC for unknown pending subscription with Id: %s" % packet_id)

    @asyncio.coroutine
    def handle_pubcomp(self, pubcomp: PubcompPacket):
        packet_id = pubcomp.variable_header.packet_id
        try:
            inflight_message = self.session.inflight_out[packet_id]
            inflight_message.received_pubcomp()
        except KeyError as ke:
            self.logger.warn("Received PUBCOMP for unknown pending subscription with Id: %s" % packet_id)

    @asyncio.coroutine
    def handle_pubrel(self, pubrel: PubrecPacket):
        packet_id = pubrel.variable_header.packet_id
        try:
            waiter = self._pubrel_waiters[packet_id]
            waiter.set_result(pubrel)
        except KeyError as ke:
            self.logger.warn("Received PUBREL for unknown pending subscription with Id: %s" % packet_id)

    @asyncio.coroutine
    def handle_publish(self, publish : PublishPacket):
        inflight_message = None
        packet_id = publish.variable_header.packet_id
        qos = (publish.fixed_header.flags >> 1) & 0x03

        if qos == 0:
            inflight_message = InFlightMessage(publish, qos)
            yield from self.delivered_message.put(inflight_message)
        else:
            if packet_id in self.session.inflight_in:
                inflight_message = self.session.inflight_in[packet_id]
            else:
                inflight_message = InFlightMessage(publish, qos)
                self.session.inflight_in[packet_id] = inflight_message
                inflight_message.publish()

            if qos == 1:
                puback = PubackPacket.build(packet_id)
                yield from self.outgoing_queue.put(puback)
                inflight_message.acknowledge()
            if qos == 2:
                pubrec = PubrecPacket.build(packet_id)
                yield from self.outgoing_queue.put(pubrec)
                inflight_message.receive()
                waiter = futures.Future(loop=self._loop)
                self._pubrel_waiters[packet_id] = waiter
                yield from waiter
                inflight_message.pubrel = waiter.result()
                del self._pubrel_waiters[packet_id]
                inflight_message.release()
                pubcomp = PubcompPacket.build(packet_id)
                yield from self.outgoing_queue.put(pubcomp)
                inflight_message.complete()
            yield from self.delivered_message.put(inflight_message)
            del self.session.inflight_in[packet_id]
