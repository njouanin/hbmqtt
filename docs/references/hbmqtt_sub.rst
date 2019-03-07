hbmqtt_sub
==========

``hbmqtt_sub`` is a command line MQTT client that subscribes to some topics and output data received from messages published.

Usage
-----

``hbmqtt_sub`` usage : ::

  hbmqtt_sub --version
  hbmqtt_sub (-h | --help)
  hbmqtt_sub --url BROKER_URL -t TOPIC... [-n COUNT] [-c CONFIG_FILE] [-i CLIENT_ID] [-q | --qos QOS] [-d]
             [-k KEEP_ALIVE] [--clean-session] [--ca-file CAFILE] [--ca-path CAPATH] [--ca-data CADATA]
             [ --will-topic WILL_TOPIC [--will-message WILL_MESSAGE] [--will-qos WILL_QOS] [--will-retain] ]
             [--extra-headers HEADER]

Note that for simplicity, ``hbmqtt_sub`` uses mostly the same argument syntax as `mosquitto_sub`_.

Options
-------

--version           HBMQTT version information
-h, --help          Display ``hbmqtt_sub`` usage help
-c                  Set the YAML configuration file to read and pass to the client runtime.
-d                  Enable debugging informations.
--ca-file           Define the path to a file containing PEM encoded CA certificates that are trusted. Used to enable SSL communication.
--ca-path           Define the path to a directory containing PEM encoded CA certificates that are trusted. Used to enable SSL communication.
--ca-data           Set the PEM encoded CA certificates that are trusted. Used to enable SSL communication.
--clean-session     If given, set the CONNECT clean session flag to True.
-i                  The id to use for this client. If not given, defaults to ``hbmqtt_sub/`` appended with the process id and the hostname of the client.
-k                  Set the CONNECT keep alive timeout.
-n                  Number of messages to read before ending. Read forever if not given.
-q, --qos           Specify the quality of service to use for receiving messages. This QoS is sent in the subscribe request.
-t                  Topic filters to subcribe.
--url               Broker connection URL, conforming to `MQTT URL scheme`_.
--will-topic        The topic on which to send a Will, in the event that the client disconnects unexpectedly.
--will-message      Specify a message that will be stored by the broker and sent out if this client disconnects unexpectedly. This must be used in conjunction with ``--will-topic``.
--will-qos          The QoS to use for the Will. Defaults to 0. This must be used in conjunction with ``--will-topic``.
--will-retain       If given, if the client disconnects unexpectedly the message sent out will be treated as a retained message. This must be used in conjunction with ``--will-topic``.
--extra-headers     Specify a JSON object string with key-value pairs representing additional headers that are transmitted on the initial connection, but only when using a websocket connection


.. _MQTT URL scheme: https://github.com/mqtt/mqtt.github.io/wiki/URI-Scheme


Configuration
-------------

If ``-c`` argument is given, ``hbmqtt_sub`` will read specific MQTT settings for the given configuration file. This file must be a valid `YAML`_ file which may contains the following configuration elements :

* ``keep_alive`` : Keep-alive timeout sent to the broker. Defaults to ``10`` seconds.
* ``ping_delay`` : Auto-ping delay before keep-alive timeout. Defaults to 1. Setting to ``0`` will disable to 0 and may lead to broker disconnection.
* ``default_qos`` : Default QoS for messages published. Defaults to 0.
* ``default_retain`` : Default retain value to messages published. Defaults to ``false``.
* ``auto_reconnect`` : Enable or disable auto-reconnect if connectection with the broker is interrupted. Defaults to ``false``.
* ``reconnect_retries`` : Maximum reconnection retries. Defaults to ``2``. Negative value will cause client to reconnect infinietly.
* ``reconnect_max_interval`` : Maximum interval between 2 connection retry. Defaults to ``10``.


.. _YAML: http://yaml.org/

Examples
--------

Examples below are adapted from `mosquitto_sub`_ documentation.


Subscribe with QoS 0 to all messages published under $SYS/:
::

    hbmqtt_sub --url mqtt://localhost -t '$SYS/#' -q 0


Subscribe to 10 messages with QoS 2 from /#:
::

    hbmqtt_sub --url mqtt://localhost -t /# -q 2 -n 10

.. _mosquitto_sub : http://mosquitto.org/man/mosquitto_sub-1.html

Subscribe with QoS 0 to all messages published under $SYS/: over mqtt encapsulated in a websocket connection and additional headers:
::

    hbmqtt_sub --url wss://localhost -t '$SYS/#' -q 0 --extra-headers '{"Authorization": "Bearer <token>"}'
