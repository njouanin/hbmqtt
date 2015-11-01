HBMQTT
======

``HBMQTT`` is an open source `MQTT`_ client and broker implementation. It implements `MQTT 3.1.1`_ protocol specifications

Built on top of :mod:`asyncio`, Python's standard asynchronous I/O framework, ``HBMQTT`` provides a straightforward API based on coroutines, making it easy to write
highly concurrent applications.

Requirements
------------

``HBMQTT`` is built on Python :mod:`asyncio` library which was introduced in Python 3.4. Tests shown that ``HBMQTT`` runs best on Python 3.4.3. Python 3.5.0 is also fully supported and recommended.

Installation
------------

It is not recommended to install third-party library in Python system packages directory. The preferred way for installing ``HBMQTT`` is to create a virtual environment and then install all the dependencies you need. Refer `PEP 405`_ to learn more.

Once you have a environment setup and ready, ``HBMQTT`` can be installed with the following command ::

  (venv) $ pip install hbmqtt

pip will download and install ``HBMQTT`` and all its dependencies.


User guide
----------



.. _MQTT: http://www.mqtt.org
.. _MQTT 3.1.1: http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
.. _PEP 405: https://www.python.org/dev/peps/pep-0405/

.. toctree::
   :maxdepth: 2
   :hidden:

