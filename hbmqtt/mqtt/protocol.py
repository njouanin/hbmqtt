# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import logging
import asyncio
from hbmqtt.mqtt.packet import MQTTFixedHeader
from hbmqtt.mqtt import packet_class
from hbmqtt.errors import NoDataException
from hbmqtt.mqtt.packet import PacketType
from hbmqtt.mqtt.connect import ConnectVariableHeader, ConnectPacket, ConnectPayload
from hbmqtt.mqtt.disconnect import DisconnectPacket
from hbmqtt.mqtt.pingreq import PingReqPacket
from hbmqtt.mqtt.pingresp import PingRespPacket
from hbmqtt.session import Session
from blinker import Signal

class ProtocolHandler:
    """
    Class implementing the MQTT communication protocol using asyncio features
    """
    packet_sent = Signal()
    packet_received = Signal()

    def __init__(self, session: Session, loop=None):
        self.logger = logging.getLogger(__name__)
        self.session = session
        if loop is None:
            self._loop = asyncio.get_event_loop()
        else:
            self._loop = loop
        self._reader_task = None
        self._writer_task = None
        self._reader_ready = asyncio.Event(loop=self._loop)
        self._writer_ready = asyncio.Event(loop=self._loop)
        self._running = False

        self.session.local_address, self.session.local_port = self.session.writer.get_extra_info('sockname')
        self.incoming_queues = dict()
        for p in PacketType:
            self.incoming_queues[p] = asyncio.Queue()
        self.outgoing_queue = asyncio.Queue()

    @asyncio.coroutine
    def start(self):
        self._running = True
        self._reader_task = asyncio.async(self._reader_coro(), loop=self._loop)
        self._writer_task = asyncio.async(self._writer_coro(), loop=self._loop)
        yield from asyncio.wait([self._reader_ready.wait(), self._writer_ready.wait()], loop=self._loop)
        self.logger.debug("Handler tasks started")


    @asyncio.coroutine
    def stop(self):
        self._running = False
        self.session.reader.feed_eof()
        yield from asyncio.wait([self._writer_task, self._reader_task], loop=self._loop)

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
                    yield from self.incoming_queues[packet.fixed_header.packet_type].put(packet)
                    self.packet_received.send(packet)
                else:
                    self.logger.debug("No data")
            except asyncio.TimeoutError:
                self.logger.debug("Input stream read timeout")
            except NoDataException as nde:
                self.logger.debug("No data available")
                #break
            except Exception as e:
                self.logger.warn("Unhandled exception in reader coro: %s" % e)
                break
        self.logger.debug("Reader coro stopped")


    @asyncio.coroutine
    def _writer_coro(self):
        self.logger.debug("Starting writer coro")
        while self._running:
            try:
                self._writer_ready.set()
                packet = yield from asyncio.wait_for(self.outgoing_queue.get(), 5)
                self.logger.debug(" -out-> " + repr(packet))
                yield from packet.to_stream(self.session.writer)
                yield from self.session.writer.drain()
                self.packet_sent.send(packet)
            except asyncio.TimeoutError as ce:
                self.logger.debug("Output queue get timeout")
            except Exception as e:
                self.logger.warn("Unhandled exception in writer coro: %s" % e)
                break
        self.logger.debug("Writer coro stopping")
        # Flush queue before stopping
        if not self.outgoing_queue.empty():
            while True:
                try:
                    packet = self.outgoing_queue.get_nowait()
                    self.logger.debug(packet)
                    yield from packet.to_stream(self.session.writer)
                except asyncio.QueueEmpty:
                    break
                except Exception as e:
                    self.logger.warn("Unhandled exception in writer coro: %s" % e)
        self.logger.debug("Writer coro stopped")


class ClientProtocolHandler(ProtocolHandler):
    def __init__(self, session: Session, loop=None):
        super().__init__(session, loop)
        self._ping_task = None

    @asyncio.coroutine
    def start(self):
        yield from super().start()
        self.packet_sent.connect(self._do_keepalive)

    @asyncio.coroutine
    def stop(self):
        if self._ping_task:
            try:
                self._ping_task.cancel()
            except Exception:
                pass
        yield from super().stop()

    def _do_keepalive(self, message):
        if self._ping_task:
            try:
                self._ping_task.cancel()
                self.logger.debug('Cancel pending ping')
            except Exception:
                pass
        next_ping = self.session.keep_alive #-self.config['ping_delay']
        if next_ping > 0:
            self.logger.debug('Next ping in %d seconds' % next_ping)
            self._ping_task = self._loop.call_later(next_ping, asyncio.async, self.mqtt_ping())

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
        connack = yield from self.incoming_queues[PacketType.CONNACK].get()

        return connack.variable_header.return_code

    @asyncio.coroutine
    def mqtt_disconnect(self):
        disconnect_packet = DisconnectPacket()
        yield from self.outgoing_queue.put(disconnect_packet)
        self._ping_task.cancel()

    @asyncio.coroutine
    def mqtt_ping(self):
        self.logger.debug("Pinging ...")
        ping_packet = PingReqPacket()
        yield from self.outgoing_queue.put(ping_packet)
        yield from self.incoming_queues[PacketType.PINGRESP].get()


