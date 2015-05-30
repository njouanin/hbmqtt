# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest
import logging
import time
from hbmqtt.broker import Broker

logging.basicConfig(level=logging.DEBUG)

class TestBroker(unittest.TestCase):
    def test_start_broker(self):
        b = Broker()
        b.start()
        time.sleep(100)
        self.assertEqual(b.machine.state, 'started')
        b.shutdown()