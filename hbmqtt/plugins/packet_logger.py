# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.

import logging
import asyncio
from functools import partial


class PacketLoggerPlugin:
    def __init__(self, context):
        self.logger = logging.getLogger(__name__)
        self.context = context

    @asyncio.coroutine
    def on_mqtt_packet_received(self, *args, **kwargs):
        packet = kwargs.get('packet')
        session = kwargs.get('session', None)
        if session:
            self.logger.debug("%s <-in-- %s" % (session.client_id, repr(packet)))
        else:
            self.logger.debug("<-in-- %s" % repr(packet))

    @asyncio.coroutine
    def on_mqtt_packet_sent(self, *args, **kwargs):
        packet = kwargs.get('packet')
        session = kwargs.get('session', None)
        if session:
            self.logger.debug("%s -out-> %s" % (session.client_id, repr(packet)))
        else:
            self.logger.debug("-out-> %s" % repr(packet))
