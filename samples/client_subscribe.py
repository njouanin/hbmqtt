import logging
import asyncio

from hbmqtt.client import MQTTClient



#
# This sample shows how to subscbribe a topic and receive data from incoming messages
# It subscribes to '$SYS/broker/uptime' topic and displays the first ten values returned
# by the broker.
#

logger = logging.getLogger(__name__)

C = MQTTClient()

@asyncio.coroutine
def uptime_coro():
    yield from C.connect(uri='mqtt://test.mosquitto.org:1883/', username=None, password=None)
    # Subscribe to '$SYS/broker/uptime' with QOS=1
    yield from C.subscribe([
                 {'filter': '$SYS/broker/uptime', 'qos': 0x01},
                 {'filter': '$SYS/broker/load/#', 'qos': 0x00},
             ])
    logger.info("Subscribed")
    for i in range(1, 10):
        inflight = yield from C.deliver_message()
        print(inflight.packet.payload.data)
    yield from C.unsubscribe(['$SYS/broker/uptime'])
    logger.info("UnSubscribed")
    yield from C.disconnect()


if __name__ == '__main__':
    formatter = "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=formatter)
    asyncio.get_event_loop().run_until_complete(uptime_coro())