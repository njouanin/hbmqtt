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

    @asyncio.coroutine
    def start(self):
        self._reader_task = asyncio.async(self._reader_coro(), loop=self._loop)
        self._writer_task = asyncio.async(self._writer_coro(), loop=self._loop)
        yield from asyncio.wait([self._reader_ready.wait(), self._writer_ready.wait()], loop=self._loop)
        self.logger.debug("Handler tasks started")

    @asyncio.coroutine
    def stop(self):
        self._reader_task.cancel()
        self._writer_task.cancel()

    @asyncio.coroutine
    def _reader_coro(self):
        self.logger.debug("Starting reader coro")
        while True:
            try:
                self._reader_ready.set()
                fixed_header = yield from MQTTFixedHeader.from_stream(self.session.reader)
                cls = packet_class(fixed_header)
                packet = yield from cls.from_stream(self.session.reader, fixed_header=fixed_header)
                yield from self.session.incoming_queues[packet.fixed_header.packet_type].put(packet)
            except asyncio.CancelledError:
                self.logger.warn("Reader coro stopped")
                break
            except NoDataException:
                self.logger.debug("No more data to read")
                break
            except Exception as e:
                self.logger.warn("Exception in reader coro: %s" % e)
                break

    @asyncio.coroutine
    def _writer_coro(self):
        self.logger.debug("Starting writer coro")
        out_queue = self.session.outgoing_queue
        while True:
            try:
                self._writer_ready.set()
                packet = yield from out_queue.get()
                self.logger.debug(packet)
                yield from packet.to_stream(self.session.writer)
            except asyncio.CancelledError:
                self.logger.warn("Writer coro stopping")
                # Flush queue
                while True:
                    try:
                        packet = out_queue.get_nowait()
                        self.logger.debug(packet)
                        yield from packet.to_stream(self.session.writer)
                    except asyncio.QueueEmpty:
                        break
                    self.logger.warn("Writer coro stopped")
                break
            except Exception as e:
                self.logger.warn("Exception in writer coro: %s" % e)
                break
