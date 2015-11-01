# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.


import logging
import asyncio
from functools import partial


class EventLoggerPlugin:
    def __init__(self, context):
        self.context = context

    @asyncio.coroutine
    def log_event(self, *args, **kwargs):
        self.context.logger.info("### '%s' EVENT FIRED ###" % kwargs['event_name'].replace('old', ''))

    def __getattr__(self, name):
        if name.startswith("on_"):
            return partial(self.log_event, event_name=name)


class PacketLoggerPlugin:
    def __init__(self, context):
        self.context = context

    @asyncio.coroutine
    def on_mqtt_packet_received(self, *args, **kwargs):
        packet = kwargs.get('packet')
        session = kwargs.get('session', None)
        if self.context.logger.isEnabledFor(logging.DEBUG):
            if session:
                self.context.logger.debug("%s <-in-- %s" % (session.client_id, repr(packet)))
            else:
                self.context.logger.debug("<-in-- %s" % repr(packet))

    @asyncio.coroutine
    def on_mqtt_packet_sent(self, *args, **kwargs):
        packet = kwargs.get('packet')
        session = kwargs.get('session', None)
        if self.context.logger.isEnabledFor(logging.DEBUG):
            if session:
                self.context.logger.debug("%s -out-> %s" % (session.client_id, repr(packet)))
            else:
                self.context.logger.debug("-out-> %s" % repr(packet))
