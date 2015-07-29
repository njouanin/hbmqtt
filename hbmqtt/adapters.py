# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import asyncio
from websockets.protocol import WebSocketCommonProtocol
from asyncio import StreamReader, StreamWriter
from hbmqtt.mqtt.packet import MQTTFixedHeader


class ProtocolAdapter:
    """
    Base class for all network protocol adapter.

    Protocol adapters are used to adapt read/write operations on the network depending on the protocol used
    """

    @asyncio.coroutine
    def read_packet(self) -> bytes:
        """
        Read a MQTT packet from the protocol.
        :return: packet read as vytes data
        """

    @asyncio.coroutine
    def write_packet(self, data: bytes):
        """
        Writes a MQTT packet bytes to the protocol implementation
        :param data: Bytes data to write
        :return:
        """

    @asyncio.coroutine
    def close(self):
        """
        Close network connection
        :return:
        """


class WebSocketsAdapter(ProtocolAdapter):
    """
    WebSockets API protocol adapter
    This adapter relies on WebSocketCommonProtocol to read/write to/from a WebSocket.
    """
    def __init__(self, protocol: WebSocketCommonProtocol):
        self._protocol = protocol
        self._message_buffer = b''

    @asyncio.coroutine
    def read_packet(self) -> bytes:
        if len(self._message_buffer) < 5:
            yield from self._feed_buffer()
        fixed_header = MQTTFixedHeader.from_bytes(self._message_buffer)
        packet_length = fixed_header.bytes_length + fixed_header.remaining_length
        self._feed_buffer(fixed_header.bytes_length + fixed_header.remaining_length)

        header_offset = fixed_header.bytes_length
        packet_length = fixed_header.bytes_length + fixed_header.remaining_length
        data = self._message_buffer[header_offset:packet_length]
        self._message_buffer = self._message_buffer[packet_length:]
        return fixed_header, data

    @asyncio.coroutine
    def _feed_buffer(self, n=1):
        """
        Feed the data buffer by reading a Websocket message.
        :param n: if given, feed buffer until it contains at least n bytes
        """
        while len(self._message_buffer) < n:
            message = yield from self._protocol.recv()
            if not type(message, bytes):
                raise TypeError("message must be bytes")
            self._message_buffer += message

    @asyncio.coroutine
    def write_packet(self, data: bytes):
        yield from self._protocol.send(data)

    @asyncio.coroutine
    def close(self):
        yield from self._protocol.close()


class StreamProtocolAdapter(ProtocolAdapter):
    """
    Asyncio Streams API protocol adapter
    This adapter relies on StreamReader and StreamWriter to read/write to/from a TCP socket.
    """
    def __init__(self, reader: StreamReader, writer: StreamWriter):
        self._reader = reader
        self._writer = writer

    @asyncio.coroutine
    def read_packet(self, n=-1) -> bytes:
        header_bytes = yield from self._reader.read(5)
        fixed_header = MQTTFixedHeader.from_bytes(header_bytes)
        data = yield from self._reader.read(fixed_header.remaining_length)
        return fixed_header, data

    @asyncio.coroutine
    def write_packet(self, data: bytes):
        try:
            self._writer.write(data)
            yield from self._writer.drain()
        except ConnectionResetError:
            self.close()

    @asyncio.coroutine
    def close(self):
        self._writer.close()
