import sqlite3
import time

import psutil

import sruns_monitor.utils as utils


class Db:
    RUNS_TABLE = "runs"
    RUN_STATUSES = {}
    RUN_STATUSES["new"] = "new"
    RUN_STATUSES["complete"] = "complete"
    RUN_STATUSES["running"] = "running"
    RUN_STATUSES["start_tar"] = "start_tar"
    RUN_STATUSES["start_upload"] = "start_upload"
    TASKS_TABLE_NAME = "tasks"
    # tasks attribute names
    TASKS_NAME = "name"
    TASKS_PID = "pid"
    TASKS_TAR_STATUS = "tar_status"
    TASKS_UPLOAD_STATUS = "upload_status"

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
        self.conn = sqlite3.connect(dbname)
        self.curs = self.conn.cursor()
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS {table} (
                {name} text PRIMARY KEY,
                {pid} integer,
                {tar_status} integer,
                {upload_status} integer);
            """.format(table=self.TASKS_TABLE_NAME,  
                       name=self.TASKS_NAME, 
                       pid=self.TASKS_PID,
                       tar_status=self.TASKS_TAR_STATUS,
                       upload_status=self.TASKS_UPLOAD_STATUS)

        self.curs.execute(create_table_sql)

    def insert_run(self, name, pid, tar_status, upload_status):
        sql = """
              INSERT INTO {tasks}({name_attr},{pid_attr},{tar_status_attr},{upload_status_attr})
              VALUES({name},{pid},{tar_status},{upload_status})
              """.format(
                  tasks=self.TASKS_TABLE_NAME,
                  name_attr=self.TASKS_NAME,
                  pid_attr=self.TASKS_PID,
                  tar_status_attr=self.TASKS_TAR_STATUS,
                  upload_status_attr=upload_status_attr,
                  name=name,
                  pid=pid,
                  tar_status=tar_status,
                  upload_stats=upload_status)
        self.curs.execute(sql)
              

    def get_run(self, name):
        """
        Returns:
            `tuple`: A record whose name attribute has the supplied name exists. 
            `None`: No such record exists.
        """
        sql = "SELECT {name},{pid},{tar_status},{upload_status} FROM {tasks} WHERE {name}={input_name}".format(
            name=self.TASKS_NAME, 
            pid=self.TASKS_PID,
            tar_status=self.TASKS_TAR_STATUS, 
            upload_status=self.TASKS_UPLOAD_STATUS, 
            tasks=self.TASKS_TABLE_NAME,
            input_name=name)

        res = self.curs.execute(sql).fetchone()
        if not res:
            return {}
        return {
            self.TASKS_NAME: res[0],
            self.TASKS_PID: res[1],
            self.TASKS_TAR_STATUS: res[2],
            self.TASKS_UPLOAD_STATUS: res[3]
        }

    def delete_run(self, name):
        sql = "DELETE FROM {tasks} WHERE {name}={input_name}".format(
            tasks=self.TASKS_TABLE_NAME,
            name=self.TASKS_NAME, 
            input_name=name)
        self.curs.execute(sql)
