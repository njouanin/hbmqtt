# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import logging
import asyncio
from asyncio import futures
from hbmqtt.mqtt.packet import MQTTFixedHeader
from hbmqtt.mqtt import packet_class
from hbmqtt.errors import NoDataException
from hbmqtt.mqtt.packet import PacketType
from hbmqtt.mqtt.connect import ConnectVariableHeader, ConnectPacket, ConnectPayload
from hbmqtt.mqtt.connack import ConnackPacket
from hbmqtt.mqtt.disconnect import DisconnectPacket
from hbmqtt.mqtt.pingreq import PingReqPacket
from hbmqtt.mqtt.publish import PublishPacket
from hbmqtt.mqtt.pubrel import PubrelPacket
from hbmqtt.mqtt.puback import PubackPacket
from hbmqtt.mqtt.pubrec import PubrecPacket
from hbmqtt.mqtt.pubcomp import PubcompPacket
from hbmqtt.mqtt.subscribe import SubscribePacket
from hbmqtt.mqtt.suback import SubackPacket
from hbmqtt.mqtt.unsubscribe import UnsubscribePacket
from hbmqtt.mqtt.unsuback import UnsubackPacket
from hbmqtt.session import Session
from transitions import Machine, MachineError

class InFlightMessage:
    states = ['new', 'published', 'acknowledged', 'received', 'released', 'completed']

    def __init__(self, packet_id, qos):
        self.packet_id = packet_id
        self.qos = qos
        self._init_states()

    def _init_states(self):
        self.machine = Machine(model=self, states=InFlightMessage.states, initial='new')
        self.machine.add_transition(trigger='publish', source='new', dest='published')
        if self.qos == 0x01:
            self.machine.add_transition(trigger='acknowledge', source='published', dest='acknowledged')
        if self.qos == 0x02:
            self.machine.add_transition(trigger='receive', source='published', dest='received')
            self.machine.add_transition(trigger='release', source='received', dest='released')
            self.machine.add_transition(trigger='complete', source='released', dest='completed')


class ProtocolHandler:
    """
    Class implementing the MQTT communication protocol using asyncio features
    """

    def __init__(self, session: Session, config, loop=None):
        self.logger = logging.getLogger(__name__)
        self.session = session
        self.config = config
        if loop is None:
            self._loop = asyncio.get_event_loop()
        else:
            self._loop = loop
        self._reader_task = None
        self._writer_task = None
        self._inflight_task = None
        self._reader_ready = asyncio.Event(loop=self._loop)
        self._writer_ready = asyncio.Event(loop=self._loop)
        self._inflight_ready = asyncio.Event(loop=self._loop)
        self._inflight_changed = asyncio.Condition(loop=self._loop)

        self._running = False

        self.session.local_address, self.session.local_port = self.session.writer.get_extra_info('sockname')

        self.incoming_queues = dict()
        self.application_messages = asyncio.Queue()
        for p in PacketType:
            self.incoming_queues[p] = asyncio.Queue()
        self.outgoing_queue = asyncio.Queue()
        self.inflight_messages = dict()

    @asyncio.coroutine
    def start(self):
        self._running = True
        self._reader_task = asyncio.async(self._reader_coro(), loop=self._loop)
        self._writer_task = asyncio.async(self._writer_coro(), loop=self._loop)
        self._inflight_task = asyncio.async(self._inflight_coro(), loop=self._loop)
        yield from asyncio.wait(
            [self._reader_ready.wait(), self._writer_ready.wait(), self._inflight_ready.wait()], loop=self._loop)
        self.logger.debug("Handler tasks started")

    @asyncio.coroutine
    def mqtt_publish(self, topic, message, packet_id, dup, qos, retain):
        def qos_0_predicate():
            ret = False
            try:
                if self.inflight_messages.get(packet_id).state == 'published':
                    ret = True
                #self.logger.debug("qos_0 predicate return %s" % ret)
                return ret
            except KeyError:
                return False

        def qos_1_predicate():
            ret = False
            try:
                if self.inflight_messages.get(packet_id).state == 'acknowledged':
                    ret = True
                #self.logger.debug("qos_1 predicate return %s" % ret)
                return ret
            except KeyError:
                return False

        def qos_2_predicate():
            ret = False
            try:
                if self.inflight_messages.get(packet_id).state == 'completed':
                    ret = True
                #self.logger.debug("qos_1 predicate return %s" % ret)
                return ret
            except KeyError:
                return False

        if packet_id in self.inflight_messages:
            self.logger.warn("A message with the same packet ID is already in flight")
        packet = PublishPacket.build(topic, message, packet_id, dup, qos, retain)
        yield from self.outgoing_queue.put(packet)
        inflight_message = InFlightMessage(packet.variable_header.packet_id, qos)
        inflight_message.publish()
        self.inflight_messages[packet.variable_header.packet_id] = inflight_message
        yield from self._inflight_changed.acquire()
        if qos == 0x00:
            yield from self._inflight_changed.wait_for(qos_0_predicate)
        if qos == 0x01:
            yield from self._inflight_changed.wait_for(qos_1_predicate)
        if qos == 0x02:
            yield from self._inflight_changed.wait_for(qos_2_predicate)
        self.inflight_messages.pop(packet.variable_header.packet_id)
        self._inflight_changed.release()
        return packet

    @asyncio.coroutine
    def stop(self):
        self._running = False
        self.session.reader.feed_eof()
        yield from asyncio.wait([self._inflight_task, self._writer_task, self._reader_task], loop=self._loop)

    @asyncio.coroutine
    def _reader_coro(self):
        self.logger.debug("Starting reader coro")
        while self._running:
            try:
                self._reader_ready.set()
                fixed_header = yield from asyncio.wait_for(MQTTFixedHeader.from_stream(self.session.reader), 5)
                if fixed_header:
                    cls = packet_class(fixed_header)
                    packet = yield from cls.from_stream(self.session.reader, fixed_header=fixed_header)
                    self.logger.debug(" <-in-- " + repr(packet))

                    if packet.fixed_header.packet_type == PacketType.CONNACK:
                        yield from self.handle_connack(packet)
                    if packet.fixed_header.packet_type == PacketType.SUBACK:
                        yield from self.handle_suback(packet)
                    if packet.fixed_header.packet_type == PacketType.UNSUBACK:
                        yield from self.handle_unsuback(packet)
                    else:
                        yield from self.incoming_queues[packet.fixed_header.packet_type].put(packet)
                else:
                    self.logger.debug("No more data, stopping reader coro")
                    break
            except asyncio.TimeoutError:
                self.logger.debug("Input stream read timeout")
            except NoDataException as nde:
                self.logger.debug("No data available")
            except Exception as e:
                self.logger.warn("Unhandled exception in reader coro: %s" % e)
                break
        self.logger.debug("Reader coro stopped")

    @asyncio.coroutine
    def _writer_coro(self):
        self.logger.debug("Starting writer coro")
        keepalive_timeout = self.session.keep_alive - self.config['ping_delay']
        while self._running:
            try:
                self._writer_ready.set()
                packet = yield from asyncio.wait_for(self.outgoing_queue.get(), keepalive_timeout)
                yield from packet.to_stream(self.session.writer)
                self.logger.debug(" -out-> " + repr(packet))
                yield from self.session.writer.drain()
                #self.outgoing_queue.task_done() # to be used with Python 3.5
            except asyncio.TimeoutError as ce:
                self.logger.debug("Output queue get timeout")
                if self._running:
                    self.logger.debug("PING for keepalive")
                    self.handle_keepalive()
            except Exception as e:
                self.logger.warn("Unhandled exception in writer coro: %s" % e)
                break
        self.logger.debug("Writer coro stopping")
        # Flush queue before stopping
        if not self.outgoing_queue.empty():
            while True:
                try:
                    packet = self.outgoing_queue.get_nowait()
                    yield from packet.to_stream(self.session.writer)
                    self.logger.debug(" -out-> " + repr(packet))
                except asyncio.QueueEmpty:
                    break
                except Exception as e:
                    self.logger.warn("Unhandled exception in writer coro: %s" % e)
        self.logger.debug("Writer coro stopped")

    @asyncio.coroutine
    def _inflight_coro(self):
        self.logger.debug("Starting in-flight messages polling coro")
        while self._running:
            self._inflight_ready.set()
            yield from asyncio.sleep(self.config['inflight-polling-interval'])
            self.logger.debug("in-flight polling coro wake-up")
            try:
                while not self.incoming_queues[PacketType.PUBACK].empty():
                    packet = self.incoming_queues[PacketType.PUBACK].get_nowait()
                    packet_id = packet.variable_header.packet_id
                    inflight_message = self.inflight_messages.get(packet_id)
                    inflight_message.acknowledge()
                    self.logger.debug("Message with packet Id=%s acknowledged" % packet_id)

                while not self.incoming_queues[PacketType.PUBREC].empty():
                    packet = self.incoming_queues[PacketType.PUBREC].get_nowait()
                    packet_id = packet.variable_header.packet_id
                    inflight_message = self.inflight_messages.get(packet_id)
                    inflight_message.receive()
                    self.logger.debug("Message with packet Id=%s received" % packet_id)

                    rel_packet = PubrelPacket.build(packet_id)
                    yield from self.outgoing_queue.put(rel_packet)
                    inflight_message.release()
                    self.logger.debug("Message with packet Id=%s released" % packet_id)

                while not self.incoming_queues[PacketType.PUBCOMP].empty():
                    packet = self.incoming_queues[PacketType.PUBCOMP].get_nowait()
                    packet_id = packet.variable_header.packet_id
                    inflight_message = self.inflight_messages.get(packet_id)
                    inflight_message.complete()
                    self.logger.debug("Message with packet Id=%s completed" % packet_id)

                yield from self._inflight_changed.acquire()
                self._inflight_changed.notify_all()
                self._inflight_changed.release()
            except KeyError:
                self.logger.warn("Received %s for unknown inflight message Id %d" % (packet.fixed_header.packet_type, packet_id))
            except MachineError as me:
                self.logger.warn("Packet type incompatible with message QOS: %s" % me)
        self.logger.debug("In-flight messages polling coro stopped")

    @asyncio.coroutine
    def _receive_publish_coro(self):
        while self._running:
            message = yield from self.incoming_queues[PacketType.PUBLISH].get()
            yield self.application_messages.put(message)
            message_id = message.fixed_header.packet_id
            if (message.fixed_header.flags >> 1) & 0x01:
                # QOS 1
                yield from self.outgoing_queue.put(PubackPacket.build(message_id))
            if (message.fixed_header.flags >> 1) & 0x02:
                # QOS 2
                yield from self.outgoing_queue.put(PubrecPacket.build(message_id))

    @asyncio.coroutine
    def mqtt_deliver_next_message(self):
        message = yield from self.application_messages.get()
        message_id = message.fixed_header.packet_id
        if (message.fixed_header.flags >> 1) & 0x02:
            # QOS 2
            yield from self.outgoing_queue.put(PubrecPacket.build(message_id))
        return message

    def handle_keepalive(self):
        pass

    @asyncio.coroutine
    def handle_connack(self, connack: ConnackPacket):
        pass

    @asyncio.coroutine
    def handle_suback(self, suback: SubackPacket):
        pass

    @asyncio.coroutine
    def handle_unsuback(self, unsuback: UnsubackPacket):
        pass


class ClientProtocolHandler(ProtocolHandler):
    def __init__(self, session: Session, config, loop=None):
        super().__init__(session, config, loop)
        self._ping_task = None
        self._connack_waiter = None
        self._subscriptions_waiter = dict()
        self._unsubscriptions_waiter = dict()

    @asyncio.coroutine
    def start(self):
        yield from super().start()

    @asyncio.coroutine
    def stop(self):
        yield from super().stop()
        if self._ping_task:
            try:
                self._ping_task.cancel()
            except Exception:
                pass

    def handle_keepalive(self):
        self._ping_task = self._loop.call_soon(asyncio.async, self.mqtt_ping())

    @asyncio.coroutine
    def mqtt_subscribe(self, topics, packet_id):
        """
        :param topics: array of topics [{'filter':'/a/b', 'qos': 0x00}, ...]
        :return:
        """
        subscribe = SubscribePacket.build(topics, packet_id)
        yield from self.outgoing_queue.put(subscribe)
        waiter = futures.Future(loop=self._loop)
        self._subscriptions_waiter[subscribe.variable_header.packet_id] = waiter
        return_codes = yield from waiter
        del self._subscriptions_waiter[subscribe.variable_header.packet_id]
        return return_codes

    @asyncio.coroutine
    def handle_suback(self, suback: SubackPacket):
        packet_id = suback.variable_header.packet_id
        try:
            waiter = self._subscriptions_waiter.get(packet_id)
            waiter.set_result(suback.payload.return_codes)
        except KeyError as ke:
            self.logger.warn("Received SUBACK for unknown pending subscription with Id: %s" % packet_id)

    @asyncio.coroutine
    def mqtt_unsubscribe(self, topics, packet_id):
        """

        :param topics: array of topics ['/a/b', ...]
        :return:
        """
        unsubscribe = UnsubscribePacket.build(topics, packet_id)
        yield from self.outgoing_queue.put(unsubscribe)
        waiter = futures.Future(loop=self._loop)
        self._unsubscriptions_waiter[unsubscribe.variable_header.packet_id] = waiter
        yield from waiter
        del self._unsubscriptions_waiter[unsubscribe.variable_header.packet_id]

    @asyncio.coroutine
    def handle_unsuback(self, unsuback: UnsubackPacket):
        packet_id = unsuback.variable_header.packet_id
        try:
            waiter = self._unsubscriptions_waiter.get(packet_id)
            waiter.set_result(None)
        except KeyError as ke:
            self.logger.warn("Received UNSUBACK for unknown pending subscription with Id: %s" % packet_id)

    @asyncio.coroutine
    def mqtt_connect(self):
        def build_connect_packet(session):
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

            header = MQTTFixedHeader(PacketType.CONNECT, 0x00)
            packet = ConnectPacket(header, vh, payload)
            return packet

        packet = build_connect_packet(self.session)
        yield from self.outgoing_queue.put(packet)
        self._connack_waiter = futures.Future(loop=self._loop)
        return (yield from self._connack_waiter)

    @asyncio.coroutine
    def handle_connack(self, connack: ConnackPacket):
        self._connack_waiter.set_result(connack.variable_header.return_code)

    @asyncio.coroutine
    def mqtt_disconnect(self):
        # yield from self.outgoing_queue.join() To be used in Python 3.5
        disconnect_packet = DisconnectPacket()
        yield from self.outgoing_queue.put(disconnect_packet)
        self._connack_waiter = None

    @asyncio.coroutine
    def mqtt_ping(self):
        ping_packet = PingReqPacket()
        yield from self.outgoing_queue.put(ping_packet)
        yield from self.incoming_queues[PacketType.PINGRESP].get()
