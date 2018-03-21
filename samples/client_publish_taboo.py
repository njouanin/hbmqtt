import logging
import asyncio

from hbmqtt.client import MQTTClient, ConnectException


#
# This sample shows how to publish messages to broker using different QOS
# Debug outputs shows the message flows
#

logger = logging.getLogger(__name__)


@asyncio.coroutine
def test_coro():
    try:
        C = MQTTClient()
        yield from C.connect('mqtt://0.0.0.0:1883')
        yield from C.publish('data/classified', b'TOP SECRET', qos=0x01)
        yield from C.publish('data/memes', b'REAL FUN', qos=0x01)
        logger.info("messages published")
        yield from C.disconnect()
    except ConnectException as ce:
        logger.error("Connection failed: %s" % ce)
        asyncio.get_event_loop().stop()


if __name__ == '__main__':
    formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
    formatter = "%(message)s"
    logging.basicConfig(level=logging.DEBUG, format=formatter)
    asyncio.get_event_loop().run_until_complete(test_coro())
