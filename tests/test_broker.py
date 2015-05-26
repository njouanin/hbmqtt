import unittest
import logging
import time
from hbmqtt.broker import Broker

logging.basicConfig(level=logging.DEBUG)

class TestBroker(unittest.TestCase):
    def test_start_broker(self):
        b = Broker()
        b.start()
        time.sleep(1)
        self.assertEqual(b.machine.state, 'started')
        b.shutdown()