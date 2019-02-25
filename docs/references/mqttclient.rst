MQTTClient API
==============

The :class:`~hbmqtt.client.MQTTClient` class implements the client part of MQTT protocol. It can be used to publish and/or subscribe MQTT message on a broker accessible on the network through TCP or websocket protocol, both secured or unsecured.


Usage examples
--------------

Subscriber
..........

The example below shows how to write a simple MQTT client which subscribes a topic and prints every messages received from the broker :

.. code-block:: python

    import logging
    import asyncio

    from hbmqtt.client import MQTTClient, ClientException
    from hbmqtt.mqtt.constants import QOS_1, QOS_2
    
    logger = logging.getLogger(__name__)

    @asyncio.coroutine
    def uptime_coro():
        C = MQTTClient()
        yield from C.connect('mqtt://test.mosquitto.org/')
        # Subscribe to '$SYS/broker/uptime' with QOS=1
        # Subscribe to '$SYS/broker/load/#' with QOS=2
        yield from C.subscribe([
                ('$SYS/broker/uptime', QOS_1),
                ('$SYS/broker/load/#', QOS_2),
             ])
        try:
            for i in range(1, 100):
                message = yield from C.deliver_message()
                packet = message.publish_packet
                print("%d:  %s => %s" % (i, packet.variable_header.topic_name, str(packet.payload.data)))
            yield from C.unsubscribe(['$SYS/broker/uptime', '$SYS/broker/load/#'])
            yield from C.disconnect()
        except ClientException as ce:
            logger.error("Client exception: %s" % ce)

    if __name__ == '__main__':
        formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
        logging.basicConfig(level=logging.DEBUG, format=formatter)
        asyncio.get_event_loop().run_until_complete(uptime_coro())

When executed, this script gets the default event loop and asks it to run the ``uptime_coro`` until it completes.
``uptime_coro`` starts by initializing a :class:`~hbmqtt.client.MQTTClient` instance.
The coroutine then call :meth:`~hbmqtt.client.MQTTClient.connect` to connect to the broker, here ``test.mosquitto.org``.
Once connected, the coroutine subscribes to some topics, and then wait for 100 messages. Each message received is simply written to output.
Finally, the coroutine unsubscribes from topics and disconnects from the broker.

Publisher
.........

The example below uses the :class:`~hbmqtt.client.MQTTClient` class to implement a publisher.
This test publish 3 messages asynchronously to the broker on a test topic.
For the purposes of the test, each message is published with a different Quality Of Service.
This example also shows to method for publishing message asynchronously.

.. code-block:: python

    import logging
    import asyncio

    from hbmqtt.client import MQTTClient
    from hbmqtt.mqtt.constants import QOS_0, QOS_1, QOS_2

    logger = logging.getLogger(__name__)
    
    @asyncio.coroutine
    def test_coro():
        C = MQTTClient()
        yield from C.connect('mqtt://test.mosquitto.org/')
        tasks = [
            asyncio.ensure_future(C.publish('a/b', b'TEST MESSAGE WITH QOS_0')),
            asyncio.ensure_future(C.publish('a/b', b'TEST MESSAGE WITH QOS_1', qos=QOS_1)),
            asyncio.ensure_future(C.publish('a/b', b'TEST MESSAGE WITH QOS_2', qos=QOS_2)),
        ]
        yield from asyncio.wait(tasks)
        logger.info("messages published")
        yield from C.disconnect()


    @asyncio.coroutine
    def test_coro2():
        try:
            C = MQTTClient()
            ret = yield from C.connect('mqtt://test.mosquitto.org:1883/')
            message = yield from C.publish('a/b', b'TEST MESSAGE WITH QOS_0', qos=QOS_0)
            message = yield from C.publish('a/b', b'TEST MESSAGE WITH QOS_1', qos=QOS_1)
            message = yield from C.publish('a/b', b'TEST MESSAGE WITH QOS_2', qos=QOS_2)
            #print(message)
            logger.info("messages published")
            yield from C.disconnect()
        except ConnectException as ce:
            logger.error("Connection failed: %s" % ce)
            asyncio.get_event_loop().stop()


    if __name__ == '__main__':
        formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
        logging.basicConfig(level=logging.DEBUG, format=formatter)
        asyncio.get_event_loop().run_until_complete(test_coro())
        asyncio.get_event_loop().run_until_complete(test_coro2())

As usual, the script runs the publish code through the async loop. ``test_coro()`` and ``test_coro()`` are ran in sequence.
Both do the same job. ``test_coro()`` publish 3 messages in sequence. ``test_coro2()`` publishes the same message asynchronously.
The difference appears the looking at the sequence of MQTT messages sent.

``test_coro()`` achieves:
::

    hbmqtt/YDYY;NNRpYQSy3?o -out-> PublishPacket(ts=2015-11-11 21:54:48.843901, fixed=MQTTFixedHeader(length=28, flags=0x0), variable=PublishVariableHeader(topic=a/b, packet_id=None), payload=PublishPayload(data="b'TEST MESSAGE WITH QOS_0'"))
    hbmqtt/YDYY;NNRpYQSy3?o -out-> PublishPacket(ts=2015-11-11 21:54:48.844152, fixed=MQTTFixedHeader(length=30, flags=0x2), variable=PublishVariableHeader(topic=a/b, packet_id=1), payload=PublishPayload(data="b'TEST MESSAGE WITH QOS_1'"))
    hbmqtt/YDYY;NNRpYQSy3?o <-in-- PubackPacket(ts=2015-11-11 21:54:48.979665, fixed=MQTTFixedHeader(length=2, flags=0x0), variable=PacketIdVariableHeader(packet_id=1), payload=None)
    hbmqtt/YDYY;NNRpYQSy3?o -out-> PublishPacket(ts=2015-11-11 21:54:48.980886, fixed=MQTTFixedHeader(length=30, flags=0x4), variable=PublishVariableHeader(topic=a/b, packet_id=2), payload=PublishPayload(data="b'TEST MESSAGE WITH QOS_2'"))
    hbmqtt/YDYY;NNRpYQSy3?o <-in-- PubrecPacket(ts=2015-11-11 21:54:49.029691, fixed=MQTTFixedHeader(length=2, flags=0x0), variable=PacketIdVariableHeader(packet_id=2), payload=None)
    hbmqtt/YDYY;NNRpYQSy3?o -out-> PubrelPacket(ts=2015-11-11 21:54:49.030823, fixed=MQTTFixedHeader(length=2, flags=0x2), variable=PacketIdVariableHeader(packet_id=2), payload=None)
    hbmqtt/YDYY;NNRpYQSy3?o <-in-- PubcompPacket(ts=2015-11-11 21:54:49.092514, fixed=MQTTFixedHeader(length=2, flags=0x0), variable=PacketIdVariableHeader(packet_id=2), payload=None)fixed=MQTTFixedHeader(length=2, flags=0x0), variable=PacketIdVariableHeader(packet_id=2), payload=None)

while ``test_coro2()`` runs:
::

    hbmqtt/LYRf52W[56SOjW04 -out-> PublishPacket(ts=2015-11-11 21:54:48.466123, fixed=MQTTFixedHeader(length=28, flags=0x0), variable=PublishVariableHeader(topic=a/b, packet_id=None), payload=PublishPayload(data="b'TEST MESSAGE WITH QOS_0'"))
    hbmqtt/LYRf52W[56SOjW04 -out-> PublishPacket(ts=2015-11-11 21:54:48.466432, fixed=MQTTFixedHeader(length=30, flags=0x2), variable=PublishVariableHeader(topic=a/b, packet_id=1), payload=PublishPayload(data="b'TEST MESSAGE WITH QOS_1'"))
    hbmqtt/LYRf52W[56SOjW04 -out-> PublishPacket(ts=2015-11-11 21:54:48.466695, fixed=MQTTFixedHeader(length=30, flags=0x4), variable=PublishVariableHeader(topic=a/b, packet_id=2), payload=PublishPayload(data="b'TEST MESSAGE WITH QOS_2'"))
    hbmqtt/LYRf52W[56SOjW04 <-in-- PubackPacket(ts=2015-11-11 21:54:48.613062, fixed=MQTTFixedHeader(length=2, flags=0x0), variable=PacketIdVariableHeader(packet_id=1), payload=None)
    hbmqtt/LYRf52W[56SOjW04 <-in-- PubrecPacket(ts=2015-11-11 21:54:48.661073, fixed=MQTTFixedHeader(length=2, flags=0x0), variable=PacketIdVariableHeader(packet_id=2), payload=None)
    hbmqtt/LYRf52W[56SOjW04 -out-> PubrelPacket(ts=2015-11-11 21:54:48.661925, fixed=MQTTFixedHeader(length=2, flags=0x2), variable=PacketIdVariableHeader(packet_id=2), payload=None)
    hbmqtt/LYRf52W[56SOjW04 <-in-- PubcompPacket(ts=2015-11-11 21:54:48.713107, fixed=MQTTFixedHeader(length=2, flags=0x0), variable=PacketIdVariableHeader(packet_id=2), payload=None)

Both coroutines have the same results except that ``test_coro2()`` manages messages flow in parallel which may be more efficient.

Reference
---------

MQTTClient API
..............

.. automodule:: hbmqtt.client

    .. autoclass:: MQTTClient

        .. automethod:: connect
        .. automethod:: disconnect
        .. automethod:: reconnect
        .. automethod:: ping
        .. automethod:: publish
        .. automethod:: subscribe
        .. automethod:: unsubscribe
        .. automethod:: deliver_message

MQTTClient configuration
........................

The :class:`~hbmqtt.client.MQTTClient` ``__init__`` method accepts a ``config`` parameter which allow to setup some behaviour and defaults settings. This argument must be a Python dict object which may contain the following entries:

* ``keep_alive``: keep alive (in seconds) to send when connecting to the broker (defaults to ``10`` seconds). :class:`~hbmqtt.client.MQTTClient` will *auto-ping* the broker if not message is sent within the keep-alive interval. This avoids disconnection from the broker.
* ``ping_delay``: *auto-ping* delay before keep-alive times out (defaults to ``1`` seconds).
* ``default_qos``: Default QoS (``0``) used by :meth:`~hbmqtt.client.MQTTClient.publish` if ``qos`` argument is not given.
* ``default_retain``: Default retain (``False``) used by :meth:`~hbmqtt.client.MQTTClient.publish` if ``qos`` argument is not given.,
* ``auto_reconnect``: enable or disable auto-reconnect feature (defaults to ``True``).
* ``reconnect_max_interval``: maximum interval (in seconds) to wait before two connection retries (defaults to ``10``).
* ``reconnect_retries``: maximum number of connect retries (defaults to ``2``). Negative value will cause client to reconnect infinietly.
Default QoS and default retain can also be overriden by adding a ``topics`` with may contain QoS and retain values for specific topics. See the following example:

.. code-block:: python

    config = {
        'keep_alive': 10,
        'ping_delay': 1,
        'default_qos': 0,
        'default_retain': False,
        'auto_reconnect': True,
        'reconnect_max_interval': 5,
        'reconnect_retries': 10,
        'topics': {
            '/test': { 'qos': 1 },
            '/some_topic': { 'qos': 2, 'retain': True }
        }
    }

With this setting any message published will set with QOS_0 and retain flag unset except for :

* messages sent to ``/test`` topic : they will be sent with QOS_1
* messages sent to ``/some_topic`` topic : they will be sent with QOS_2 and retain flag set

In any case, the ``qos`` and ``retain`` argument values passed to method :meth:`~hbmqtt.client.MQTTClient.publish` will override these settings.
