Changelog
---------

0.7.2
.....

* fix deliver message client method to raise TimeoutError (`#40 <https://github.com/beerfactory/hbmqtt/issues/40>`_)
* fix topic filter matching in broker (`#41 <https://github.com/beerfactory/hbmqtt/issues/41>`_)

0.7.1
.....

* Fix `duplicated $SYS topic name <https://github.com/beerfactory/hbmqtt/issues/37>`_ .

0.7.0
.....

* Fix a `serie of issues <https://github.com/beerfactory/hbmqtt/issues?q=milestone%3A0.7+is%3Aclosed>`_ reported by `Christoph Krey <https://github.com/ckrey>`_

0.6.3
.....

* Fix issue `#22 <https://github.com/beerfactory/hbmqtt/issues/22>`_.

0.6.2
.....

* Fix issue `#20 <https://github.com/beerfactory/hbmqtt/issues/20>`_  (``mqtt`` subprotocol was missing).
* Upgrade to ``websockets`` 3.0.

0.6.1
.....

* Fix issue `#19 <https://github.com/beerfactory/hbmqtt/issues/19>`_

0.6
...

* Added compatibility with Python 3.5.
* Rewritten documentation.
* Add command-line tools :doc:`references/hbmqtt`, :doc:`references/hbmqtt_pub` and :doc:`references/hbmqtt_sub`.