# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest
import asyncio
import logging
from hbmqtt.plugins.manager import PluginManager
from hbmqtt.broker import Broker
from hbmqtt.mqtt.constants import *

formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
logging.basicConfig(level=logging.DEBUG, format=formatter)
log = logging.getLogger(__name__)


class BrokerTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.plugin_manager = PluginManager("hbmqtt.test.plugins", context=None, loop=self.loop)

    def tearDown(self):
        self.loop.close()

    def test_start_stop(self):
        config = {
            'listeners': {
                'default': {
                    'type': 'tcp'
                },
                'tcp-mqtt': {
                    'bind': '0.0.0.0:1883',
                    'max_connections': 10
                },
            },
            'sys_interval': 0,
            'auth': {
                'allow-anonymous': True,
            }
        }

        def test_coro():
            broker = Broker(config)
            yield from broker.start()
            yield from broker.shutdown()

        self.loop.run_until_complete(test_coro())
