# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import logging
import asyncio
from hbmqtt.session import Session
from hbmqtt.mqtt.packet import MQTTFixedHeader
from hbmqtt.mqtt import packet_class
from hbmqtt.errors import NoDataException

class ProtocolHandler:
    """
    Class implementing the MQTT communication protocol using asyncio features
    """
    def __init__(self, session: Session, loop):
        self.logger = logging.getLogger(__name__)
        self.session = session
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
                fixed_header = yield from asyncio.wait_for(MQTTFixedHeader.from_stream(self.session.reader), 60)
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
                packet = yield from asyncio.wait_for(out_queue.get(), 60)
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
