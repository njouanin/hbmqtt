__author__ = 'nico'

import abc
import asyncio

from hbmqtt.messages.packet import MQTTHeader
from hbmqtt.session import Session


class AbstractHandler(metaclass=abc.ABCMeta):
    def __init__(self, broker):
        self._broker = broker

    @abc.abstractmethod
    @asyncio.coroutine
    def _check_header(self, header):
        return

    @abc.abstractmethod
    @asyncio.coroutine
    def _decode_payload(self, session: Session, header: MQTTHeader):
        return

    @abc.abstractmethod
    @asyncio.coroutine
    def _process_request(self, session: Session, message):
        return

    @asyncio.coroutine
    def process_incoming_packet(self, session: Session, header: MQTTHeader):
        yield from self._check_header(header)
        message = yield from self._decode_payload(session, header)
        yield from self._process_request(session, message)