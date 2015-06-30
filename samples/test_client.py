import logging
from hbmqtt.client._client import MQTTClient
import asyncio

logger = logging.getLogger(__name__)

C = MQTTClient()

@asyncio.coroutine
def test_coro():
    yield from C.connect(uri='mqtt://iot.eclipse.org:1883/', username='testuser', password="passwd")
    tasks = [
        asyncio.async(C.publish('a/b', b'0123456789')),
        asyncio.async(C.publish('a/b', b'0', qos=0x01)),
        asyncio.async(C.publish('a/b', b'1', qos=0x01)),
        asyncio.async(C.publish('a/b', b'2', qos=0x01)),
        asyncio.async(C.publish('a/b', b'3', qos=0x01)),
        asyncio.async(C.publish('a/b', b'4', qos=0x01)),
        asyncio.async(C.publish('a/b', b'5', qos=0x01)),
        asyncio.async(C.publish('a/b', b'6', qos=0x01)),
        asyncio.async(C.publish('a/b', b'7', qos=0x01)),
        asyncio.async(C.publish('a/b', b'8', qos=0x01)),
        asyncio.async(C.publish('a/b', b'9', qos=0x01)),
        asyncio.async(C.publish('a/b', b'0', qos=0x02)),
        asyncio.async(C.publish('a/b', b'1', qos=0x02)),
        asyncio.async(C.publish('a/b', b'2', qos=0x02)),
        asyncio.async(C.publish('a/b', b'3', qos=0x02)),
        asyncio.async(C.publish('a/b', b'4', qos=0x02)),
        asyncio.async(C.publish('a/b', b'5', qos=0x02)),
        asyncio.async(C.publish('a/b', b'6', qos=0x02)),
        asyncio.async(C.publish('a/b', b'7', qos=0x02)),
        asyncio.async(C.publish('a/b', b'8', qos=0x02)),
        asyncio.async(C.publish('a/b', b'9', qos=0x02)),
    ]
    yield from asyncio.wait(tasks)
    logger.info("messages published")
    yield from C.subscribe([
                 {'filter': 'a/b', 'qos': 0x01},
                 {'filter': 'c/d', 'qos': 0x02}
             ])
    logger.info("Subscribed")
    yield from C.unsubscribe(['a/b', 'c/d'])
    logger.info("Unsubscribed")

    yield from C.disconnect()


if __name__ == '__main__':
    formatter = "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=formatter)
    asyncio.get_event_loop().run_until_complete(test_coro())