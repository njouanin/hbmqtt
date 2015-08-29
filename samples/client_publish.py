import logging
import asyncio

from hbmqtt.client import MQTTClient, ConnectException



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


def disconnected(future):
    print("DISCONNECTED")
    asyncio.get_event_loop().stop()


@asyncio.coroutine
def test_coro():
    yield from C.connect('mqtt://localhost:1883/')
    tasks = [
        asyncio.async(C.publish('a/b', b'TEST MESSAGE WITH QOS_0')),
        asyncio.async(C.publish('a/b', b'TEST MESSAGE WITH QOS_1', qos=0x01)),
        asyncio.async(C.publish('a/b', b'TEST MESSAGE WITH QOS_2', qos=0x02)),
    ]
    yield from asyncio.wait(tasks)
    logger.info("messages published")
    yield from C.disconnect()


@asyncio.coroutine
def test_coro2():
    try:
        future = yield from C.connect('mqtt://test:test@localhost:1883/')
        future.add_done_callback(disconnected)
        yield from asyncio.wait([asyncio.async(C.publish('a/b', b'TEST MESSAGE WITH QOS_1', qos=0x01))])
        logger.info("messages published")
        yield from C.disconnect()
    except ConnectException as ce:
        logger.error("Connection failed: %s" % ce)
        asyncio.get_event_loop().stop()


if __name__ == '__main__':
    formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=formatter)
    asyncio.async(test_coro2())
    try:
        asyncio.get_event_loop().run_forever()
    finally:
        asyncio.get_event_loop().close()