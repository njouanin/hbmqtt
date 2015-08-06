import logging
import asyncio
from hbmqtt.broker import Broker

logger = logging.getLogger(__name__)

config = {
    'listeners': {
        'default': {
            'type': 'tcp'
        },
        'tcp-mqtt': {
            'bind': '0.0.0.0:1883',
        },
        'ws-mqtt': {
            'bind': '127.0.0.1:8080',
            'type': 'ws'
        },
        'wss-mqtt': {
            'bind': '127.0.0.1:8081',
            'type': 'ws',
            'ssl': 'on',
            'certfile': 'localhost.server.crt',
            'keyfile': 'server.key',
        },
        'tcp-ssl': {
            'bind': '127.0.0.1:8883',
            'ssl': 'on',
            'certfile': 'localhost.server.crt',
            'keyfile': 'server.key',
            'type': 'tcp'
        }
    }
}

broker = Broker(config)

@asyncio.coroutine
def test_coro():
    yield from broker.start()
    #yield from asyncio.sleep(5)
    #yield from broker.shutdown()


if __name__ == '__main__':
    formatter = "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=formatter)
    asyncio.get_event_loop().run_until_complete(test_coro())
    asyncio.get_event_loop().run_forever()