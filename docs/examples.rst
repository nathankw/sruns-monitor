Examples
========

Starting the monitor
--------------------

You can start it using the command-line script sruns-mon or by explicitly instantiating the Monitor
class.

By script
^^^^^^^^^

::

  srun-mon -c conf.json

explicitly
^^^^^^^^^^

::

  from sruns_monitor.monitor import Monitor

  mon = Monitor(conf="conf.json")
  mon.start()

Running functional tests
------------------------
Run these tests to ensure that your configuration file is set up appropriately and that you have the
proper GCP security credentials configured. Firestore tests will only run if you have included the
parameter :const:`sruns_monitor.C_FIRESTORE_COLLECTION`.

::

  monitor_integration_tests.py


