Quickstart
==========

``HBMQTT`` provides console scripts for :

* publishing a message on some topic on an external MQTT broker.
* subscribing some topics and getting published messages.
* running a autonomous MQTT broker

These scripts are installed automatically when installing ``HBMQTT`` with the following command ::

  (venv) $ pip install hbmqtt

Read the foloowing sections to learn how to uses these scripts.

hbmqtt_pub
----------

``hbmqtt_pub`` is a MQTT client that publishes simple messages on a topic from the command line.

Usage
.....

``hbmqtt_pub`` usage : ::

  hbmqtt_pub --version
  hbmqtt_pub (-h | --help)
  hbmqtt_pub --url BROKER_URL -t TOPIC (-f FILE | -l | -m MESSAGE | -n | -s) [-c CONFIG_FILE] [-i CLIENT_ID] [-q | --qos QOS] [-d] [-k KEEP_ALIVE] [--clean-session] [--ca-file CAFILE] [--ca-path CAPATH] [--ca-data CADATA] [ --will-topic WILL_TOPIC [--will-message WILL_MESSAGE] [--will-qos WILL_QOS] [--will-retain] ]

Note that for simplicity, ``hbmqtt_pub`` uses mostly the same argument syntax as `mosquitto_pub`_.

.. _mosquitto_pub: http://mosquitto.org/man/mosquitto_pub-1.html

Options
.......

--version           HBMQTT version information
-h, --help          Display ``hbmqtt_pub`` usage help
-c                  Set the YAML configuration file to read and pass to the client runtime.
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



.. _MQTT URL scheme: https://github.com/mqtt/mqtt.github.io/wiki/URI-Scheme

Examples
........




