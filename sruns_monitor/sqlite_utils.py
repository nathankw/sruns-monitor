# -*- coding: utf-8 -*-

import logging
import sqlite3
import time

import psutil

import sruns_monitor as srm
import sruns_monitor.utils as utils
import pdb

DBG_LGR = logging.getLogger(srm.DEBUG_LOGGER_NAME)
ERR_LGR = logging.getLogger(srm.ERROR_LOGGER_NAME)

class Db:
    #: The name of the table that stores workflow state for each sequencing run. 
    TASKS_TABLE_NAME = "tasks"
    #: 'tasks' table attribute name that stores the name of the sequencing run. 
    TASKS_NAME = "name"
    #: 'tasks' table attribute name that stores the process ID of the workflow that is currently
    #: running or had run.
    TASKS_PID = "pid"
    #: 'tasks' table attribute name that stores the path to the gzip tarfile of the run directory.
    TASKS_TARFILE = "tarfile"
    #: 'tasks' table attribute name that stores the path to the gzip tarfile in a GCP Storage bucket.
    TASKS_GCP_TARFILE = "gcp_tarfile"

    def __init__(self, dbname):
        """
        Args:
            dbname: `str`. Name of the local database file. If it doesn't end with a .db exention,
                one will be added. 
        """
        if not dbname.endswith(".db"):
            dbname += ".db"
        self.dbname = dbname
        # Database is created if it doesn't exist yet. See entry level details here:
        # http://www.sqlitetutorial.net/sqlite-python/creating-database/
        DBG_LGR.error("Connecting to sqlite database {}".format(dbname))
        self.conn = sqlite3.connect(dbname)
        self.curs = self.conn.cursor()
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS {table} (
                {name} text PRIMARY KEY,
                {pid} integer,
                {tarfile} text,
                {gcp_tarfile} text);
            """.format(table=self.TASKS_TABLE_NAME,  
                       name=self.TASKS_NAME, 
                       pid=self.TASKS_PID,
                       tarfile=self.TASKS_TARFILE,
                       gcp_tarfile=self.TASKS_GCP_TARFILE)

        self.curs.execute(create_table_sql)

    def insert_run(self, name, pid=0, tarfile="", gcp_tarfile=""):
        """
        Creates a new record in the database. You most likely only need to set the name attribute
        since other attributes will be set by the workflow as it progresses. 

        Args:
            name: `str`. Value for the *name* attribute. Set this to the sequencing run name. 
            pid: `int`. Value for the *pid* attribute that should be the process ID of the workflow
                if running already. 
            tarfile: `str`. The name of the tarfile. Doesn't make sense to set if the workflow task
                that tars the run directory hasn't run yet. 
            gcp_tarfile: `str`. Blob name for the tarfile that is in GCP storage. Doesn't make sense
                to set if the workflow task that uploads the tarfile to GCP hasn't run yet. 

        Returns: None
 
        """
        sql = """
              INSERT INTO {table}({name_attr},{pid_attr},{tarfile_attr},{gcp_tarfile_attr})
              VALUES('{name}',{pid},'{tarfile}','{gcp_tarfile}')
              """.format(
                  table=self.TASKS_TABLE_NAME,
                  name_attr=self.TASKS_NAME,
                  pid_attr=self.TASKS_PID,
                  tarfile_attr=self.TASKS_TARFILE,
                  gcp_tarfile_attr=self.TASKS_GCP_TARFILE,
                  name=name,
                  pid=pid,
                  tarfile=tarfile,
                  gcp_tarfile=gcp_tarfile)
        DBG_LGR.debug(sql)
        self.curs.execute(sql) # Returns the sqlite3.Cursor object. 

    def update_run(self, name, payload):
        update_str = ""
        for attr in payload:
            update_str += "{key}='{val}',".format(key=attr, val=payload[attr])
        update_str = update_str.rstrip(",")
        sql = "UPDATE {table} SET {updates} WHERE name='{name}'".format(
            table=self.TASKS_TABLE_NAME, 
            updates=update_str,
            name=name)
        DBG_LGR.debug(sql)
        self.curs.execute(sql)
              
    def get_run(self, name):
        """
        Returns:
            `tuple`: A record whose name attribute has the supplied name exists. 
            `None`: No such record exists.
        """
        sql = "SELECT {name},{pid},{tarfile},{gcp_tarfile} FROM {table} WHERE {name}='{input_name}'".format(
            name=self.TASKS_NAME, 
            pid=self.TASKS_PID,
            tarfile=self.TASKS_TARFILE, 
            gcp_tarfile=self.TASKS_GCP_TARFILE, 
            table=self.TASKS_TABLE_NAME,
            input_name=name)

        DBG_LGR.debug(sql)
        res = self.curs.execute(sql).fetchone()
        if not res:
            return {}
        return {
            self.TASKS_NAME: res[0],
            self.TASKS_PID: res[1],
            self.TASKS_TARFILE: res[2],
            self.TASKS_GCP_TARFILE: res[3]
        }

    def delete_run(self, name):
        sql = "DELETE FROM {table} WHERE {name}='{input_name}'".format(
            table=self.TASKS_TABLE_NAME,
            name=self.TASKS_NAME, 
            input_name=name)
        DBG_LGR.debug(sql)
        self.curs.execute(sql)
