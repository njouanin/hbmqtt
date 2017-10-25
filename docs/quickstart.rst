Quickstart
==========

A quick way for getting started with ``HBMQTT`` is to use console scripts provided for :

* publishing a message on some topic on an external MQTT broker.
* subscribing some topics and getting published messages.
* running a autonomous MQTT broker

These scripts are installed automatically when installing ``HBMQTT`` with the following command ::

  (venv) $ pip3 install hbmqtt

Publishing messages
-------------------

``hbmqtt_pub`` is a command-line tool which can be used for publishing some messages on a topic. It requires a few arguments like broker URL, topic name, QoS and data to send. Additional options allow more complex use case.

Publishing ```some_data`` to as ``/test`` topic on is as simple as :
::

    $ hbmqtt_pub --url mqtt://test.mosquitto.org -t /test -m some_data
    [2015-11-06 22:21:55,108] :: INFO - hbmqtt_pub/5135-MacBook-Pro.local Connecting to broker
    [2015-11-06 22:21:55,333] :: INFO - hbmqtt_pub/5135-MacBook-Pro.local Publishing to '/test'
    [2015-11-06 22:21:55,336] :: INFO - hbmqtt_pub/5135-MacBook-Pro.local Disconnected from broker

This will use insecure TCP connection to connect to test.mosquitto.org. ``hbmqtt_pub`` also allows websockets and secure connection:
::

    $ hbmqtt_pub --url ws://test.mosquitto.org:8080 -t /test -m some_data
    [2015-11-06 22:22:42,542] :: INFO - hbmqtt_pub/5157-MacBook-Pro.local Connecting to broker
    [2015-11-06 22:22:42,924] :: INFO - hbmqtt_pub/5157-MacBook-Pro.local Publishing to '/test'
    [2015-11-06 22:22:52,926] :: INFO - hbmqtt_pub/5157-MacBook-Pro.local Disconnected from broker

``hbmqtt_pub`` can read from file or stdin and use data read as message payload:
::

    $ some_command | hbmqtt_pub --url mqtt://localhost -t /test -l

See :doc:`references/hbmqtt_pub` reference documentation for details about available options and settings.

Subscribing a topic
-------------------

``hbmqtt_sub`` is a command-line tool which can be used to subscribe for some pattern(s) on a broker and get date from messages published on topics matching these patterns by other MQTT clients.

Subscribing a ``/test/#`` topic pattern is done with :
::

  $ hbmqtt_sub --url mqtt://localhost -t /test/#

This command will run forever and print on the standard output every messages received from the broker. The ``-n`` option allows to set a maximum number of messages to receive before stopping.

See :doc:`references/hbmqtt_sub` reference documentation for details about available options and settings.


URL Scheme
----------

HBMQTT command line tools use the ``--url`` to establish a network connection with the broker. The ``--url`` parameter value must conform to the `MQTT URL scheme`_. The general accepted form is :
::

    [mqtt|ws][s]://[username][:password]@host.domain[:port]

Here are some examples of URL:
::

    mqtt://localhost
    mqtt://localhost:1884
    mqtt://user@password:localhost
    ws://test.mosquitto.org
    wss://user@password:localhost

.. _MQTT URL scheme: https://github.com/mqtt/mqtt.github.io/wiki/URI-Scheme


Running a broker
----------------

``hbmqtt`` is a command-line tool for running a MQTT broker:
::

    $ hbmqtt
    [2015-11-06 22:45:16,470] :: INFO - Listener 'default' bind to 0.0.0.0:1883 (max_connecionts=-1)

See :doc:`references/hbmqtt` reference documentation for details about available options and settings.
