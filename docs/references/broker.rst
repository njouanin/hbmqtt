Broker API reference
====================

The :class:`~hbmqtt.broker.Broker` class provides a complete MQTT 3.1.1 broker implementation. This class allows Python developers to embed a MQTT broker in their own applications.

Usage example
-------------

The following example shows how to start a broker using the default configuration:

.. code-block:: python

    import logging
    import asyncio
    import os
    from hbmqtt.broker import Broker


    @asyncio.coroutine
    def broker_coro():
        broker = Broker()
        yield from broker.start()


    if __name__ == '__main__':
        formatter = "[%(asctime)s] :: %(levelname)s :: %(name)s :: %(message)s"
        logging.basicConfig(level=logging.INFO, format=formatter)
        asyncio.get_event_loop().run_until_complete(broker_coro())
        asyncio.get_event_loop().run_forever()

When executed, this script gets the default event loop and asks it to run the ``broker_coro`` until it completes.
``broker_coro`` creates :class:`~hbmqtt.broker.Broker` instance and then :meth:`~hbmqtt.broker.Broker.start` the broker for serving.
Once completed, the loop is ran forever, making this script never stop ...

Reference
---------

Broker API
..........

.. automodule:: hbmqtt.broker

    .. autoclass:: Broker

        .. automethod:: start
        .. automethod:: shutdown

Broker configuration
....................

The :class:`~hbmqtt.broker.Broker` ``__init__`` method accepts a ``config`` parameter which allow to setup some behaviour and defaults settings. This argument must be a Python dict object. For convinience, it is presented below as a YAML file [1]_.

.. code-block:: python

    listeners:
        default:
            max-connections: 50000
            type: tcp
        my-tcp-1:
            bind: 127.0.0.1:1883
        my-tcp-2:
            bind: 1.2.3.4:1884
            max-connections: 1000
        my-tcp-ssl-1:
            bind: 127.0.0.1:8885
            ssl: on
            cafile: /some/cafile
            capath: /some/folder
            capath: certificate data
            certfile: /some/certfile
            keyfile: /some/key
        my-ws-1:
            bind: 0.0.0.0:8080
            type: ws
    timeout-disconnect-delay: 2
    auth:
        plugins: ['auth.anonymous'] #List of plugins to activate for authentication among all registered plugins
        allow-anonymous: true / false
        password-file: /some/passwd_file
    topic-check:
        enabled: true / false  # Set to False if topic filtering is not needed
        plugins: ['topic_acl'] #List of plugins to activate for topic filtering among all registered plugins
        acl:
            # username: [list of allowed topics]
            username1: ['repositories/+/master', 'calendar/#', 'data/memes']  # List of topics on which client1 can publish and subscribe
            username2: ...
            anonymous: []  # List of topics on which an anonymous client can publish and subscribe


The ``listeners`` section allows to define network listeners which must be started by the :class:`~hbmqtt.broker.Broker`. Several listeners can be setup. ``default`` subsection defines common attributes for all listeners. Each listener can have the following settings:

* ``bind``: IP address and port binding.
* ``max-connections``: Set maximum number of active connection for the listener. ``0`` means no limit.
* ``type``: transport protocol type; can be ``tcp`` for classic TCP listener or ``ws`` for MQTT over websocket.
* ``ssl`` enables (``on``) or disable secured connection over the transport protocol.
* ``cafile``, ``cadata``, ``certfile`` and ``keyfile`` : mandatory parameters for SSL secured connections.

The ``auth`` section setup authentication behaviour:

* ``plugins``: defines the list of activated plugins. Note the plugins must be defined in the ``hbmqtt.broker.plugins`` `entry point <https://pythonhosted.org/setuptools/setuptools.html#dynamic-discovery-of-services-and-plugins>`_.
* ``allow-anonymous`` : used by the internal :class:`hbmqtt.plugins.authentication.AnonymousAuthPlugin` plugin. This parameter enables (``on``) or disable anonymous connection, ie. connection without username.
* ``password-file`` : used by the internal :class:`hbmqtt.plugins.authentication.FileAuthPlugin` plugin. This parameter gives to path of the password file to load for authenticating users.

The ``topic-check`` section setup access control policies for publishing and subscribing to topics:

* ``enabled``: set to true if you want to impose an access control policy. Otherwise, set it to false.
* ``plugins``: defines the list of activated plugins. Note the plugins must be defined in the ``hbmqtt.broker.plugins`` `entry point <https://pythonhosted.org/setuptools/setuptools.html#dynamic-discovery-of-services-and-plugins>`_.
* additional parameters: depending on the plugin used for access control, additional parameters should be added.
    * In case of ``topic_acl`` plugin, the Access Control List (ACL) must be defined in the parameter ``acl``.
        * For each username, a list with the allowed topics must be defined.
        * If the client logs in anonymously, the ``anonymous`` entry within the ACL is used in order to grant/deny subscriptions.


.. [1] See `PyYAML <http://pyyaml.org/wiki/PyYAMLDocumentation>`_ for loading YAML files as Python dict.
