#!/usr/bin/env python3
# -*- coding: utf-8 -*-

###
# Nathaniel Watson
# nathanielwatson@stanfordhealthcare.org
# 2019-05-21
###

"""
Tests functions in the ``sruns_monitor.monitor`` module.
"""

import hashlib
import json
import multiprocessing
import os
import psutil
import time
import unittest

import sruns_monitor as srm
from sruns_monitor.tests import WATCH_DIR, TMP_DIR
from sruns_monitor import utils
from sruns_monitor.monitor import Monitor

SQLITE_DB = "monitortest.db"
if os.path.exists(SQLITE_DB):
    os.remove(SQLITE_DB)

CONF = {
  "watchdir": "SEQ_RUNS",
  "completed_runs_dir": "COMPLETED_RUNS",
  "sqlite_db": SQLITE_DB,
  "gcp_bucket_name": "nathankw-testcgs",
  "gcp_bucket_basedir": "/",
  "cycle_pause_sec": 30,
  "task_runtime_limit_sec": 24 * 60 * 60
}

CONF_FILE = os.path.join(TMP_DIR, "conf.json")
open(CONF_FILE, 'w').write(json.dumps(CONF))

class TestUtils(unittest.TestCase):
    """
    Tests functions in the ``sruns_monitor.utils`` module.
    """

    def setUp(self):
        self.monitor = Monitor(conf_file=CONF_FILE)

    def tearDown(self):
        if os.path.exists(SQLITE_DB):
            os.remove(SQLITE_DB)

    def test_scan(self):
        """
        Tests the function ``tar()`` for success. Calls the function to tar the test directory
        in ./data/TEST_RUN_DIR, then checks whether the md5sum of the output tarball is equal to
        the md5sum of ./data/test_run_dir.tar.gz (the latter of which was created with `tar -zcf`
        command on Mac OS X.
        """
        rundirs = self.monitor.scan()
        self.assertEqual(rundirs, ["CompletedRun1", "CompletedRun2"])

    def test_status_new_run(self):
        """
        Tests that the monitor knows that a run is brand new when the database doesn't have any
        record for it. 
        """
        status = self.monitor.get_run_status("CompletedRun1")
        self.assertEqual(status, self.monitor.db.RUN_STATUS_NEW)

    def test_status_run_complete(self):
        """
        Tests that the monitor can detect that a run is completed when its database record has
        a value set for each attribute that represents a workflow step results, i.e. the tarfile
        path and the GCP Storage object path. 
        """
        run_name = "testrun"
        self.monitor.db.insert_run(name=run_name, tarfile="run.tar.gz", gcp_tarfile="/bucket/obj.tar.gz")
        status = self.monitor.get_run_status(run_name)
        self.assertEqual(status, self.monitor.db.RUN_STATUS_COMPLETE)
 
    def test_status_not_running_1(self):
        """
        When a database record has a partially completed workflow and the PID value is not set, 
        `Monitor.get_run_status` should return the status `sqlite_utils.db.Db.RUN_STATUS_NOT_RUNNING`.
        """
        run_name = "testrun"
        self.monitor.db.insert_run(name=run_name, tarfile="run.tar.gz", gcp_tarfile="", pid=0)
        status = self.monitor.get_run_status(run_name)
        self.assertEqual(status, self.monitor.db.RUN_STATUS_NOT_RUNNING)

    def test_status_not_running_2(self):
        """
        When a database record has a partially completed workflow and the PID value is set but
        that process doens't actually exist, `Monitor.get_run_status` should return the status 
        `sqlite_utils.db.Db.RUN_STATUS_NOT_RUNNING`.
        """
        run_name = "testrun"
        self.monitor.db.insert_run(name=run_name, tarfile="run.tar.gz", gcp_tarfile="", pid=101010101010)
        status = self.monitor.get_run_status(run_name)
        self.assertEqual(status, self.monitor.db.RUN_STATUS_NOT_RUNNING)

    def test_status_running(self):
        """
        When a database record has a partially completed workflow and the PID value is set and
        that process exists, `Monitor.get_run_status` should return the status 
        `sqlite_utils.db.Db.RUN_STATUS_RUNNING`.
        """
        run_name = "testrun"
        self.monitor.db.insert_run(name=run_name, tarfile="run.tar.gz", gcp_tarfile="", pid=os.getpid())
        status = self.monitor.get_run_status(run_name)
        self.assertEqual(status, self.monitor.db.RUN_STATUS_RUNNING)


class TestTaskTar(unittest.TestCase):

    def setUp(self):
        self.monitor = Monitor(conf_file=CONF_FILE) 
        self.run_name = "CompletedRun1" # An actual test run directory
        self.tarfile_name = self.run_name + ".tar.gz"

    def tearDown(self):
        if os.path.exists(SQLITE_DB):
            os.remove(SQLITE_DB)

    def test_task_tar_pid_set(self):
        """
        Makes sure that when tarring a run directory, the pid of the child process is inserted into 
        the database record.
        """
        self.monitor.db.insert_run(name=self.run_name)
        self.monitor.task_tar(state=self.monitor.state, run_name=self.run_name) 
        rec = self.monitor.db.get_run(name=self.run_name)
        pid = rec[self.monitor.db.TASKS_PID]
        self.assertTrue(pid > 0)

    def test_task_tar_tarfile_set(self):
        """
        Makes sure that after tarring a run directory, the tarfile name is inserted into 
        the database record.
        """
        self.monitor.db.insert_run(name=self.run_name)
        self.monitor.task_tar(state=self.monitor.state, run_name=self.run_name) 
        rec = self.monitor.db.get_run(name=self.run_name)
        tarfile = rec[self.monitor.db.TASKS_TARFILE]
        self.assertTrue(bool(tarfile))

    def test_task_tar_tarfile_exists(self):
        """
        Makes sure that after tarring a run directory, the tarfile referenced in the database
        record actually exists.
        """
        self.monitor.db.insert_run(name=self.run_name)
        self.monitor.task_tar(state=self.monitor.state, run_name=self.run_name) 
        rec = self.monitor.db.get_run(name=self.run_name)
        tarfile = rec[self.monitor.db.TASKS_TARFILE]
        self.assertTrue(os.path.exists(tarfile))


class TestChildTasks(unittest.TestCase):

    def setUp(self):
        self.monitor = Monitor(conf_file=CONF_FILE)

    def test_running_too_long(self):
        
        def child_task():
            time.sleep(3)

        # Make process limit 1 second
        self.monitor.conf[srm.C_TASK_RUNTIME_LIMIT_SEC] = 1
        p = multiprocessing.Process(target=child_task)
        p.start()
        time.sleep(1)
        assert(self.monitor.running_too_long(process=psutil.Process(p.pid)), True)

    def test_not_running_too_long(self):
        
        def child_task():
            time.sleep(3)

        # Make process limit 1 second
        self.monitor.conf[srm.C_TASK_RUNTIME_LIMIT_SEC] = 5
        p = multiprocessing.Process(target=child_task)
        p.start()
        time.sleep(1)
        assert(self.monitor.running_too_long(process=psutil.Process(p.pid)), False)

if __name__ == "__main__":
    unittest.main()
