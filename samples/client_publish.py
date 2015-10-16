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


async def test_coro():
    await C.connect('mqtt://test:test@localhost:1883/')
    tasks = [
        asyncio.ensure_future(C.publish('a/b', b'TEST MESSAGE WITH QOS_0')),
        asyncio.ensure_future(C.publish('a/b', b'TEST MESSAGE WITH QOS_1', qos=0x01)),
        asyncio.ensure_future(C.publish('a/b', b'TEST MESSAGE WITH QOS_2', qos=0x02)),
    ]
    await asyncio.wait(tasks)
    logger.info("messages published")
    await C.disconnect()


async def test_coro2():
    try:
        future = await C.connect('mqtt://test.mosquitto.org:1883/')
        future.add_done_callback(disconnected)
        message = await C.publish('a/b', b'TEST MESSAGE WITH QOS_0', qos=0x00)
        message = await C.publish('a/b', b'TEST MESSAGE WITH QOS_1', qos=0x01)
        message = await C.publish('a/b', b'TEST MESSAGE WITH QOS_2', qos=0x02)
        #print(message)
        logger.info("messages published")
        await C.disconnect()
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