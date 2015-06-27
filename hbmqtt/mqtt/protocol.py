# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import logging
import asyncio
from enum import Enum
from hbmqtt.mqtt.packet import MQTTFixedHeader
from hbmqtt.mqtt import packet_class
from hbmqtt.errors import NoDataException
from hbmqtt.mqtt.packet import PacketType
from hbmqtt.mqtt.connect import ConnectVariableHeader, ConnectPacket, ConnectPayload

class SessionState(Enum):
    NEW = 0
    CONNECTED = 1
    DISCONNECTED = 2

class Session:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

        self.state = SessionState.NEW
        self.reader = None
        self.writer = None
        self.remote_address = None
        self.remote_port = None
        self.local_address = None
        self.local_port = None
        self.client_id = None
        self.clean_session = None
        self.will_flag = False
        self.will_message = None
        self.will_qos = None
        self.will_retain = None
        self.will_topic = None
        self.keep_alive = None
        self.username = None
        self.password = None
        self.scheme = None
        self._packet_id = 0

        self.incoming_queues = dict()
        for p in PacketType:
            self.incoming_queues[p] = asyncio.Queue()
        self.outgoing_queue = asyncio.Queue()

        self.handler = ProtocolHandler(self)

    @asyncio.coroutine
    def open(self, reader: asyncio.StreamReader, writer:asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer
        self.local_address, self.local_port = self.writer.get_extra_info('sockname')

        yield from self.handler.start()

    @asyncio.coroutine
    def close(self):
        yield from self.handler.stop()
        self.writer.close()

    def build_connect_packet(self):
        vh = ConnectVariableHeader()
        payload = ConnectPayload()

        vh.keep_alive = self.keep_alive
        vh.clean_session_flag = self.clean_session
        vh.will_retain_flag = self.will_retain
        payload.client_id = self.client_id

        if self.username:
            vh.username_flag = True
            payload.username = self.username
        else:
            vh.username_flag = False

        if self.password:
            vh.password_flag = True
            payload.password = self.password
        else:
            vh.password_flag = False
        if self.will_flag:
            vh.will_flag = True
            vh.will_qos = self.will_qos
            payload.will_message = self.will_message
            payload.will_topic = self.will_topic
        else:
            vh.will_flag = False

        header = MQTTFixedHeader(PacketType.CONNECT, 0x00)
        packet = ConnectPacket(header, vh, payload)
        return packet


    @property
    def next_packet_id(self):
        self._packet_id += 1
        return self._packet_id

class ProtocolHandler:
    """
    Class implementing the MQTT communication protocol using asyncio features
    """
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
        yield from asyncio.wait([self._writer_task], loop=self._loop)


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
                    yield from self.session.incoming_queues[packet.fixed_header.packet_type].put(packet)
                else:
                    self.logger.debug("No data")
            except asyncio.TimeoutError:
                self.logger.warn("Input stream read timeout")
            except NoDataException as nde:
                self.logger.debug("No data available")
                #break
            except BaseException as e:
                self.logger.warn("Exception in reader coro: %s" % e)
                break
        self.logger.debug("Reader coro stopped")


    @asyncio.coroutine
    def _writer_coro(self):
        self.logger.debug("Starting writer coro")
        out_queue = self.session.outgoing_queue
        packet = None
        while self._running:
            try:
                self._writer_ready.set()
                packet = yield from asyncio.wait_for(out_queue.get(), 5)
                self.logger.debug(packet)
                yield from packet.to_stream(self.session.writer)
                yield from self.session.writer.drain()
            except asyncio.TimeoutError as ce:
                self.logger.warn("Output queue get timeout")
            except Exception as e:
                self.logger.warn("Exception in writer coro: %s" % e)
                break
        self.logger.debug("Writer coro stopping")
        # Flush queue before stopping
        if not out_queue.empty():
            while True:
                try:
                    packet = out_queue.get_nowait()
                    self.logger.debug(packet)
                    yield from packet.to_stream(self.session.writer)
                except asyncio.QueueEmpty:
                    break
        self.logger.debug("Writer coro stopped")
