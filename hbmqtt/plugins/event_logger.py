# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.

import logging
import asyncio
from functools import partial


class EventLoggerPlugin:
    def __init__(self, context):
        self.logger = logging.getLogger(__name__)
        self.context = context

    @asyncio.coroutine
    def log_event(self, *args, **kwargs):
        self.logger.info("### '%s' EVENT FIRED ###" % kwargs['event_name'].replace('old', ''))

    def __getattr__(self, name):
        if name.startswith("on_"):
            return partial(self.log_event, event_name=name)
