import logging
from hbmqtt.client._client import MQTTClient
import asyncio

C=MQTTClient()

@asyncio.coroutine
def test_coro():
    yield from C.connect(uri='mqtt://localhost:1883/', username='testuser', password="passwd")
    yield from asyncio.sleep(1)
    yield from C.publish('a/b', b'0123456789')
    yield from asyncio.sleep(10)
    yield from C.disconnect()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    asyncio.get_event_loop().run_until_complete(test_coro())