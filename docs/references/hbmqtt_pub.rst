hbmqtt_pub
==========

``hbmqtt_pub`` is a MQTT client that publishes simple messages on a topic from the command line.

Usage
-----

``hbmqtt_pub`` usage : ::

  hbmqtt_pub --version
  hbmqtt_pub (-h | --help)
  hbmqtt_pub --url BROKER_URL -t TOPIC (-f FILE | -l | -m MESSAGE | -n | -s) [-c CONFIG_FILE] [-i CLIENT_ID] [-d]
             [-q | --qos QOS] [-d] [-k KEEP_ALIVE] [--clean-session]
             [--ca-file CAFILE] [--ca-path CAPATH] [--ca-data CADATA]
             [ --will-topic WILL_TOPIC [--will-message WILL_MESSAGE] [--will-qos WILL_QOS] [--will-retain] ]
             [--extra-headers HEADER]

Note that for simplicity, ``hbmqtt_pub`` uses mostly the same argument syntax as `mosquitto_pub`_.

.. _mosquitto_pub: http://mosquitto.org/man/mosquitto_pub-1.html

Options
-------

--version           HBMQTT version information
-h, --help          Display ``hbmqtt_pub`` usage help
-c                  Set the YAML configuration file to read and pass to the client runtime.
-d                  Enable debugging informations.
--ca-file           Define the path to a file containing PEM encoded CA certificates that are trusted. Used to enable SSL communication.
--ca-path           Define the path to a directory containing PEM encoded CA certificates that are trusted. Used to enable SSL communication.
--ca-data           Set the PEM encoded CA certificates that are trusted. Used to enable SSL communication.
--clean-session     If given, set the CONNECT clean session flag to True.
-f                  Send the contents of a file as the message. The file is read line by line, and ``hbmqtt_pub`` will publish a message for each line read.
-i                  The id to use for this client. If not given, defaults to ``hbmqtt_pub/`` appended with the process id and the hostname of the client.
-l                  Send messages read from stdin. ``hbmqtt_pub`` will publish a message for each line read. Blank lines won't be sent.
-k                  Set the CONNECT keep alive timeout.
-m                  Send a single message from the command line.
-n                  Send a null (zero length) message.
-q, --qos           Specify the quality of service to use for the message, from 0, 1 and 2. Defaults to 0.
-s                  Send a message read from stdin, sending the entire content as a single message.
-t                  The MQTT topic on which to publish the message.
--url               Broker connection URL, conforming to `MQTT URL scheme`_.
--will-topic        The topic on which to send a Will, in the event that the client disconnects unexpectedly.
--will-message      Specify a message that will be stored by the broker and sent out if this client disconnects unexpectedly. This must be used in conjunction with ``--will-topic``.
--will-qos          The QoS to use for the Will. Defaults to 0. This must be used in conjunction with ``--will-topic``.
--will-retain       If given, if the client disconnects unexpectedly the message sent out will be treated as a retained message. This must be used in conjunction with ``--will-topic``.
--extra-headers     Specify a JSON object string with key-value pairs representing additional headers that are transmitted on the initial connection, but only when using a websocket connection


.. _MQTT URL scheme: https://github.com/mqtt/mqtt.github.io/wiki/URI-Scheme


Configuration
-------------

If ``-c`` argument is given, ``hbmqtt_pub`` will read specific MQTT settings for the given configuration file. This file must be a valid `YAML`_ file which may contains the following configuration elements :

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

Examples below are adapted from `mosquitto_pub`_ documentation.


Publish temperature information to localhost with QoS 1:
::

    hbmqtt_pub --url mqtt://localhost -t sensors/temperature -m 32 -q 1

Publish timestamp and temperature information to a remote host on a non-standard port and QoS 0:
::

    hbmqtt_pub --url mqtt://192.168.1.1:1885 -t sensors/temperature -m "1266193804 32"

Publish light switch status. Message is set to retained because there may be a long period of time between light switch events:
::

    hbmqtt_pub --url mqtt://localhost -r -t switches/kitchen_lights/status -m "on"

Send the contents of a file in two ways:
::

    hbmqtt_pub --url mqtt://localhost -t my/topic -f ./data

    hbmqtt_pub --url mqtt://localhost -t my/topic -s < ./data

Publish temperature information to localhost with QoS 1 over mqtt encapsulated in a websocket connection and additional headers:
::

    hbmqtt_pub --url wss://localhost -t sensors/temperature -m 32 -q 1 --extra-headers '{"Authorization": "Bearer <token>"}'


.. _mosquitto_pub : http://mosquitto.org/man/mosquitto_pub-1.html

