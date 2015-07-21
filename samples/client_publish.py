import logging
import asyncio

from hbmqtt.client import MQTTClient



#
# This sample shows how to publish messages to broker using different QOS
# Debug outputs shows the message flows
#

logger = logging.getLogger(__name__)

config = {
    'will': {
        'topic': '/will/client',
        'message': b'Dead or alive',
        'qos': 0x01,
        'retain': True
    }
}
#C = MQTTClient(config=config)
C = MQTTClient()

@asyncio.coroutine
def test_coro():
    yield from C.connect(uri='mqtt://test.mosquitto.org:1883/', username=None, password=None)
    tasks = [
        asyncio.async(C.publish('a/b', b'TEST MESSAGE WITH QOS_0')),
        asyncio.async(C.publish('a/b', b'TEST MESSAGE WITH QOS_1', qos=0x01)),
        asyncio.async(C.publish('a/b', b'TEST MESSAGE WITH QOS_2', qos=0x02)),
    ]
    yield from asyncio.wait(tasks)
    logger.info("messages published")
    yield from C.disconnect()


if __name__ == '__main__':
    formatter = "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=formatter)
    asyncio.get_event_loop().run_until_complete(test_coro())