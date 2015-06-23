# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import logging
import asyncio
import threading
from hbmqtt.session import Session
from hbmqtt.mqtt.packet import MQTTFixedHeader
from hbmqtt.mqtt import packet_class

# class ProtoThread(threading.Thread):
#     def __init__(self, session: Session, loop: asyncio.BaseEventLoop):
#         super().__init__(name="MQTT Protocol communication thread")
#         self.logger = logging.getLogger(__name__)
#         self._loop = loop
#         self._session = session
#
#     def run(self):
#         asyncio.set_event_loop(self._loop)
#         self._loop.call_soon(asyncio.async, self._read_protocol())
#         if not self._loop.is_running():
#             self._loop.run_forever()
#
#     @asyncio.coroutine
#     def _read_protocol(self):
#         while true:

class ProtocolHandler:
    """
    Class implementing the MQTT communication protocol using asyncio features
    """
    def __init__(self, session: Session, loop: asyncio.BaseEventLoop):
        self.logger = logging.getLogger(__name__)
        self.session = session
        self._loop = loop
        self._reader_task = None
        self._writer_task = None

    def start(self):
        self._reader_task = asyncio.async(self._writer_coro(), loop=self._loop)
        self._writer_task = asyncio.async(self._reader_coro(), loop=self._loop)

    def stop(self):
        self._reader_task.cancel()
        self._writer_task.cancel()

    @asyncio.coroutine
    def _reader_coro(self):
        self.logger.debug("Starting reader coro")
        while True:
            try:
                fixed_header = yield from MQTTFixedHeader.from_stream(self.session.reader)
                cls = packet_class(fixed_header)
                packet = yield from cls.from_stream(self.session.reader, fixed_header=fixed_header)
                self.logger.debug(packet)
            except asyncio.CancelledError:
                self.logger.warn("Reader coro stopping")
                break
            except Exception as e:
                self.logger.warn("Exception in reader coro: %s" % e)
                break

    @asyncio.coroutine
    def _writer_coro(self):
        self.logger.debug("Starting writer coro")
        out_queue = self.session._out_queue
        while True:
            try:
                packet = yield from out_queue.get()
                yield from packet.to_stream(self.session.writer)
            except asyncio.CancelledError:
                self.logger.warn("Reader coro stopping")
                break
            except Exception as e:
                self.logger.warn("Exception in writer coro: %s" % e)
                break
