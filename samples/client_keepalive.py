import logging
import asyncio

from hbmqtt.client import MQTTClient


#
# This sample shows a client running idle.
# Meanwhile, keepalive is managed through PING messages sent every 5 seconds
#


logger = logging.getLogger(__name__)

config = {
    'keep_alive': 5,
    'ping_delay': 1,
}
C = MQTTClient(config=config)

@asyncio.coroutine
def test_coro():
    yield from C.connect(uri='mqtt://localhost:1883/', username=None, password=None)
    yield from asyncio.sleep(18)

    yield from C.disconnect()


if __name__ == '__main__':
    formatter = "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=formatter)
    asyncio.get_event_loop().run_until_complete(test_coro())