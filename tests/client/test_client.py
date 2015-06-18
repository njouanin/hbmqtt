import logging
from hbmqtt.client._client import MQTTClient

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    C=MQTTClient()
    C.connect(uri='mqtt://localhost:1883/', username='testuser', password="passwd")