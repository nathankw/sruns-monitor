#!/usr/bin/env python3
# -*- coding: utf-8 -*-

###
# Nathaniel Watson
# nathanielwatson@stanfordhealthcare.org
# 2019-05-21
###

"""
Tests functions in the ``sruns_monitor.sqlite_utils`` module. 
"""

import hashlib
import json
import os
import unittest

from sruns_monitor.tests import WATCH_DIR, TMP_DIR
from sruns_monitor import sqlite_utils


class TestDb(unittest.TestCase):
    """
    Tests the class `sruns_monitor.Db`. 
    """

    def setUp(self):
        self.dbfile = "test.db"
        # Instantiating the Db class should create the database and a tasks table.
        self.db = sqlite_utils.Db(self.dbfile)
    

    def tearDown(self):
        self.db.curs.close() # Prevent 'sqlite3.OperationalError: database is locked' errors.
        os.remove(self.dbfile)
      
    def test_db_exists(self):
        """
        Tests that the database file gets created when instantiating the `sruns_monitor.Db` class.
        """
        self.assertTrue(os.path.exists(self.dbfile))

    def test_db_table_exists(self):
        """
        Tests that the database contains the table specified by the class variable
        `sqlite_utils.Db.TASKS_TABLE_NAME`.  
        """
        tables = self.db.get_tables()
        self.assertTrue(sqlite_utils.Db.TASKS_TABLE_NAME in tables)

    def test_insert_run_attr_name(self):
        """
        Tests `sqlite_utls.Db.insert_run` for success when creating a new record with only the name
        attribute set. 
        """
        run_name = "first_run"
        self.db.insert_run(name=run_name)
        rec = self.db.get_run(run_name)
        expected = {
            sqlite_utils.Db.TASKS_NAME: run_name,
            sqlite_utils.Db.TASKS_PID: 0,
            sqlite_utils.Db.TASKS_TARFILE: '',
            sqlite_utils.Db.TASKS_GCP_TARFILE: ''
        }
        self.assertTrue(rec == expected)

    def test_insert_run_attrs_name_pid(self):
        """
        Tests `sqlite_utls.Db.insert_run` for success when creating a new record with only the name
        and pid attributes set. 
        """
        run_name = "first_run"
        pid = 77103
        self.db.insert_run(name=run_name, pid=pid)
        rec = self.db.get_run(run_name)
        expected = {
            sqlite_utils.Db.TASKS_NAME: run_name,
            sqlite_utils.Db.TASKS_PID: pid,
            sqlite_utils.Db.TASKS_TARFILE: '',
            sqlite_utils.Db.TASKS_GCP_TARFILE: ''
        }
        self.assertTrue(rec == expected)

    def test_update_run_attr_tarfile(self):
        """
        Tests `sqlite_utls.Db.update_run` for success when updating an existing record to add a value
        for the local tarfile path.
        """
        run_name = "first_run"
        pid = 77103
        self.db.insert_run(name=run_name, pid=pid)
        tarfile = run_name + ".tar.gz"
        self.db.update_run(name=run_name, payload={sqlite_utils.Db.TASKS_TARFILE: tarfile})
        rec = self.db.get_run(run_name)
        expected = {
            sqlite_utils.Db.TASKS_NAME: run_name,
            sqlite_utils.Db.TASKS_PID: pid,
            sqlite_utils.Db.TASKS_TARFILE: tarfile,
            sqlite_utils.Db.TASKS_GCP_TARFILE: ''
        }
        self.assertTrue(rec == expected)

    def test_update_run_attr_gcptarfile(self):
        """
        Tests `sqlite_utls.Db.update_run` for success when updating an existing record to add a value
        for the gcp_tarfile path.
        """
        run_name = "first_run"
        pid = 77103
        tarfile = run_name + ".tar.gz"
        self.db.insert_run(name=run_name, pid=pid, tarfile=tarfile)
        gcp_tarfile = "run_name/run_name.tar.gz"
        self.db.update_run(name=run_name, payload={sqlite_utils.Db.TASKS_GCP_TARFILE: gcp_tarfile})
        rec = self.db.get_run(run_name)
        print(rec)
        expected = {
            sqlite_utils.Db.TASKS_NAME: run_name,
            sqlite_utils.Db.TASKS_PID: pid,
            sqlite_utils.Db.TASKS_TARFILE: tarfile,
            sqlite_utils.Db.TASKS_GCP_TARFILE: gcp_tarfile
        }
        self.assertTrue(rec == expected)
           

if __name__ == "__main__":
    unittest.main()
