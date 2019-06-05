Examples
========

Starting the monitor
--------------------

You can start it using the command-line script sruns-mon or by explicitly instantiating the Monitor
class.

script
^^^^^^

::

  srun-mon -c conf.json

explicitly
^^^^^^^^^^

::

  from sruns_monitor.monitor import Monitor

  mon = Monitor(conf="conf.json")
  mon.start()
