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
from sruns_monitor.tests import WATCH_DIR, COMPLETED_RUNS_DIR, TMP_DIR, DATA_DIR
from sruns_monitor import utils
from sruns_monitor.monitor import Monitor

SQLITE_DB = os.path.join(os.path.dirname(__file__), "monitortest.db")
if os.path.exists(SQLITE_DB):
    os.remove(SQLITE_DB)

CONF = {
  "firestore_collection": "test_sruns",
  "watchdir": WATCH_DIR,
  "completed_runs_dir": COMPLETED_RUNS_DIR,
  "sqlite_db": SQLITE_DB,
  "gcp_bucket_name": "nathankw-testcgs",
  "gcp_bucket_basedir": "/TESTS",
  "cycle_pause_sec": 30,
  "task_runtime_limit_sec": 24 * 60 * 60
}

CONF_FILE = os.path.join(TMP_DIR, "conf.json")
open(CONF_FILE, 'w').write(json.dumps(CONF))

class TestStatus(unittest.TestCase):
    """
    Tests for `Monitor.scan` and `Monitor.get_run_status`.
    """

    def setUp(self):
        """
        Create a `Monitor` instance before each test runs.  The SQLite database specified by the
        conf file is created when the Monitor is instantiated. 
        """
        self.monitor = Monitor(conf_file=CONF_FILE)

    def tearDown(self):
        """
        Remove local SQLite database after each test runs.
        """
        self.monitor.db.curs.close() # Prevent 'sqlite3.OperationalError: database is locked' errors.
        if os.path.exists(SQLITE_DB):
            os.remove(SQLITE_DB)

    def test_scan(self):
        """
        Tests `Monitor.scan` for success. It should find only the completed run directories.
        """
        rundirs = self.monitor.scan()
        self.assertEqual(rundirs, ["CompletedRun1", "CompletedRun2", "TEST_RUN_DIR"])

    def test_status_new_run(self):
        """
        Tests that the monitor knows that a run is brand new when the database doesn't have any
        record for it.
        """
        status = self.monitor.get_run_status("CompletedRun1")
        self.assertEqual(status, self.monitor.RUN_STATUS_NEW)

    def test_status_run_complete(self):
        """
        Tests that the monitor can detect that a run has completed when its database record has
        a value set for each attribute that represents a workflow step result, i.e. the tarfile
        path and the GCP Storage object path. `Monitor.get_run_status` should return the status 
        `self.Monitor.RUN_STATUS_COMPLETE`.
        """
        run_name = "testrun"
        self.monitor.db.insert_run(name=run_name, tarfile="run.tar.gz", gcp_tarfile="/bucket/obj.tar.gz")
        status = self.monitor.get_run_status(run_name)
        self.assertEqual(status, self.monitor.RUN_STATUS_COMPLETE)

    def test_status_not_running_1(self):
        """
        When a database record has a partially completed workflow and the PID value is not set,
        `Monitor.get_run_status` should return the status `self.Monitor.RUN_STATUS_NOT_RUNNING`.
        """
        run_name = "testrun"
        self.monitor.db.insert_run(name=run_name, tarfile="run.tar.gz", gcp_tarfile="", pid=0)
        status = self.monitor.get_run_status(run_name)
        self.assertEqual(status, self.monitor.RUN_STATUS_NOT_RUNNING)

    def test_status_not_running_2(self):
        """
        When a database record has a partially completed workflow and the PID value is set but
        that process doesn't actually exist, `Monitor.get_run_status` should return the status
        `self.Monitor.RUN_STATUS_NOT_RUNNING`.
        """
        run_name = "testrun"
        self.monitor.db.insert_run(name=run_name, tarfile="run.tar.gz", gcp_tarfile="", pid=1010101010)
        status = self.monitor.get_run_status(run_name)
        self.assertEqual(status, self.monitor.RUN_STATUS_NOT_RUNNING)

    def test_status_running(self):
        """
        When a database record has a partially completed workflow and the PID value is set and
        that process exists, `Monitor.get_run_status` should return the status
        `self.Monitor.RUN_STATUS_STARTING`.
        """
        run_name = "testrun"
        self.monitor.db.insert_run(name=run_name, tarfile="run.tar.gz", gcp_tarfile="", pid=os.getpid())
        status = self.monitor.get_run_status(run_name)
        self.assertEqual(status, self.monitor.RUN_STATUS_STARTING)


class TestTaskTar(unittest.TestCase):

    def setUp(self):
        """
        Creates a `Monitor` instance before each test runs.  The SQLite database specified by the 
        conf file is created when the Monitor is instantiated, and a record is written with only
        its name attribute set. Then, the tar task executes. Once that completes, the SQLite
        record is stored as `self.rec` for inspection in the various test methods. 
        """
        self.monitor = Monitor(conf_file=CONF_FILE)
        self.run_name = "CompletedRun1" # An actual test run directory
        self.tarfile_name = self.run_name + ".tar.gz"
        self.monitor.db.insert_run(name=self.run_name)
        self.monitor.task_tar(state=self.monitor.state, run_name=self.run_name, lock=self.monitor.lock)
        self.rec = self.monitor.db.get_run(name=self.run_name)

    def tearDown(self):
        """
        Remove local SQLite database after each test runs.
        """
        if os.path.exists(SQLITE_DB):
            os.remove(SQLITE_DB)

    def test_task_tar_pid_set(self):
        """
        Makes sure that when tarring a run directory, the pid of the child process is inserted into
        the SQLite record. Note in this case, the tarring method is run in the same thread, which
        is fine since this test is chiefly concerned with verifying that the pid attribute
        of the record is set. 
        """
        pid = self.rec[self.monitor.db.TASKS_PID]
        self.assertTrue(pid > 0)

    def test_task_tar_tarfile_set(self):
        """
        Makes sure that after tarring a run directory, the tarfile name is inserted into
        the SQLite record.
        """
        tarfile = self.rec[self.monitor.db.TASKS_TARFILE]
        self.assertTrue(bool(tarfile))

    def test_task_tar_tarfile_exists(self):
        """
        Makes sure that after tarring a run directory, the tarfile referenced in the database
        record actually exists.
        """
        tarfile = self.rec[self.monitor.db.TASKS_TARFILE]
        self.assertTrue(os.path.exists(tarfile))


class TestTaskUpload(unittest.TestCase):
    """
    Test's the monitor's upload-to-GCP-Storage application logic.
    """

    def setUp(self):
        """
        Create a `Monitor` instance before each test runs.  The SQLite database specified by the 
        conf file is created when the Monitor is instantiated. A run record is inserted into
        the database with a value already set for the local tarfile path, which points to a tarfile
        that is also created each time before a test runs as `Monitor.task_upload` removes the local
        tarfile before exiting. 
        """
        self.monitor = Monitor(conf_file=CONF_FILE)
        self.run_name = "CompletedRun1" # An actual test run directory
        self.tarfile = os.path.join(DATA_DIR, "rundir.tar.gz")
        fh = open(self.tarfile, 'w')
        fh.write("test line")
        fh.close()
        self.monitor.db.insert_run(name=self.run_name, tarfile=self.tarfile)

    def tearDown(self):
        """
        Remove local SQLite database after each test runs, as well as the blob object that is
        created in GCP Storage.
        """
        if os.path.exists(SQLITE_DB):
            os.remove(SQLITE_DB)
        blob = self.monitor.create_blob_name(run_name=self.run_name, filename=self.tarfile)
        self.monitor.bucket.delete_blob(blob) # raises google.api_core.exceptions.NotFound if blob doesn't exist.

    def test_task_upload_pid_set(self):
        """
        Makes sure that when uploading a tarball to GCP, the pid of the child process is inserted into
        the database record.
        """
        self.monitor.task_upload(state=self.monitor.state, run_name=self.run_name, lock=self.monitor.lock)
        rec = self.monitor.db.get_run(name=self.run_name)
        pid = rec[self.monitor.db.TASKS_PID]
        self.assertTrue(pid > 0)

    def test_task_upload_tarfile_removed(self):
        """
        Makes sure that after uploading a tarball to GCP, the local tarfile is removed. Note that
        the local record's `self.monitor.db.TASKS_TARFILE` attribute value is not changed, rather
        the file is just removed.
        """
        self.monitor.task_upload(state=self.monitor.state, run_name=self.run_name, lock=self.monitor.lock)
        rec = self.monitor.db.get_run(name=self.run_name)
        tarfile = rec[self.monitor.db.TASKS_TARFILE]
        self.assertFalse(os.path.exists(tarfile))

    def test_task_upload_gcp_tarfile_set(self):
        """
        Makes sure that after uploading a tarred run directory to GCP, the SQLite database record's
        `self.monitor.db.TASKS_GCP_TARFILE` attribute is set. Note that it should be set to the
        object's name in GCP, but this part of the logic isn't tested in this method.
        """
        self.monitor.task_upload(state=self.monitor.state, lock=self.monitor.lock, run_name=self.run_name)
        rec = self.monitor.db.get_run(name=self.run_name)
        gcp_tarfile = rec[self.monitor.db.TASKS_GCP_TARFILE]
        self.assertTrue(bool(gcp_tarfile))

    def test_task_upload_gcp_tarfile_exists(self):
        """
        Makes sure that after uploading a tarred run directory to GCP, the GCP object referenced in
        the local database record's `self.monitor.db.TASKS_GCP_TARFILE` attribute actually exists
        at the indicated location.
        """
        self.monitor.task_upload(state=self.monitor.state, lock=self.monitor.lock, run_name=self.run_name)
        rec = self.monitor.db.get_run(name=self.run_name)
        gcp_tarfile = rec[self.monitor.db.TASKS_GCP_TARFILE]
        # gcp_tarfile has 'bucket_name/' at the beginnig of the path - need to remove that.
        gcp_tarfile = gcp_tarfile.split("/", 1)[-1]
        blob = self.monitor.bucket.get_blob(gcp_tarfile)
        # blob is None if file doesn't exist in GCP, otherwise it's a Blob instance.
        self.assertTrue(bool(blob))


class TestFirestore(unittest.TestCase):

    def setUp(self):
        """
        Create a `Monitor` instance before each test runs.  The SQLite database specified by the 
        conf file is created when the Monitor is instantiated. A run record is inserted into
        the local SQLite database with a value set for the name only. Also, a Firestore record is 
        created that initially sets the run name attribute, as well as the workflow status attribute 
        to starting.
        
        """
        self.monitor = Monitor(conf_file=CONF_FILE)
        self.run_name = "CompletedRun1" # An actual test run directory
        self.monitor.db.insert_run(name=self.run_name)
        # Create Firestore document
        firestore_payload = {
            srm.FIRESTORE_ATTR_WF_STATUS: self.monitor.RUN_STATUS_STARTING
        }
        self.monitor.firestore_coll.document(self.run_name).set(firestore_payload)

    def tearDown(self):
        """
        Remove local SQLite database after each test runs, as well as the related Firestore record.
        """
        if os.path.exists(SQLITE_DB):
            os.remove(SQLITE_DB)
        self.monitor.firestore_coll.document(self.run_name).delete()

    def test_status_tar_complete(self):
        """
        Tests that after tarring is complete, the Firestore record's `sruns_monitor.FIRESTORE_ATTR_WF_STATUS`
        attribute is set to `Monitor.RUN_STATUS_TARRING_COMPLETE`. 
        """
        self.monitor.task_tar(state=self.monitor.state, run_name=self.run_name, lock=self.monitor.lock)
        doc_ref = self.monitor.firestore_coll.document(self.run_name).get()
        doc = doc_ref.to_dict()
        self.assertEqual(doc[srm.FIRESTORE_ATTR_WF_STATUS], self.monitor.RUN_STATUS_TARRING_COMPLETE)

    def test_status_upload_complete(self):
        """
        Tests that after uploading the tarfile to GCP is complete, the Firestore record's 
        `sruns_monitor.FIRESTORE_ATTR_WF_STATUS` attribute is set to `Monitor.RUN_STATUS_UPLOADING_COMPLETE`. 
        """
        self.monitor.task_upload(state=self.monitor.state, run_name=self.run_name, lock=self.monitor.lock)
        doc_ref = self.monitor.firestore_coll.document(self.run_name).get()
        doc = doc_ref.to_dict()
        self.assertEqual(doc[srm.FIRESTORE_ATTR_WF_STATUS], self.monitor.RUN_STATUS_UPLOADING_COMPLETE)

    def test_status_complete(self):
        """
        Tests that after running the entire workflow, the Firestore record's 
        `sruns_monitor.FIRESTORE_ATTR_WF_STATUS` attribute is set to `Monitor.RUN_STATUS_COMPLETE`. 

        Calls `monitor.Monitor.process_completed_run()`, which updates two attributes in Firestore:

          * `sruns_monitor.FIRESTORE_ATTR_WF_STATUS`
          * `sruns_monitor.FIRESTORE_ATTR_STORAGE`

        The first is set to completed status, and the latter is set to the value of the local database
        record's `sqlite_utils.Db.TASKS_GCP_TARFILE` attribute. This method tests that the first
        attribute's value is what we expect.
        """
        self.monitor.process_completed_run(run_name=self.run_name, archive=False)
        fs_doc_ref = self.monitor.firestore_coll.document(self.run_name).get()
        fs_doc = fs_doc_ref.to_dict()
        local_rec = self.monitor.db.get_run(name=self.run_name)
        self.assertEqual(fs_doc[srm.FIRESTORE_ATTR_WF_STATUS], self.monitor.RUN_STATUS_COMPLETE)

    def test_tarfile_path(self):
        """
        Tests that after running the entire workflow, the Firestore record's 
        `sruns_monitor.FIRESTORE_ATTR_STORAGE` attribute is set to the same value as designated in
        the local SQLite record. 

        Calls `monitor.Monitor.process_completed_run()`, which updates two attributes in Firestore:

          * `sruns_monitor.FIRESTORE_ATTR_WF_STATUS`
          * `sruns_monitor.FIRESTORE_ATTR_STORAGE`

        The first is set to completed status, and the latter is set to the value of the local database
        record's `sqlite_utils.Db.TASKS_GCP_TARFILE` attribute. This method tests that the second
        attribute's value is what we expect.
        """
        self.monitor.process_completed_run(run_name=self.run_name, archive=False)
        fs_doc_ref = self.monitor.firestore_coll.document(self.run_name).get()
        fs_doc = fs_doc_ref.to_dict()
        local_rec = self.monitor.db.get_run(name=self.run_name)
        self.assertEqual(fs_doc[srm.FIRESTORE_ATTR_STORAGE], local_rec[self.monitor.db.TASKS_GCP_TARFILE])


class TestChildTasksRuntime(unittest.TestCase):

    def setUp(self):
        self.monitor = Monitor(conf_file=CONF_FILE)

    def test_running_too_long(self):
        """
        Tests that the method `monitor.Monitor.running_too_long` returns True when a child task
        runs for more than the configured amount of time.
        """

        def child_task():
            time.sleep(3)

        # Make process limit 1 second
        self.monitor.conf[srm.C_TASK_RUNTIME_LIMIT_SEC] = 1
        p = multiprocessing.Process(target=child_task)
        p.start()
        time.sleep(1)
        assert(self.monitor.running_too_long(process=psutil.Process(p.pid)), True)

    def test_not_running_too_long(self):
        """
        Tests that the method `monitor.Monitor.running_too_long` returns False when a child task
        runs for less than the configured amount of time.
        """

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
