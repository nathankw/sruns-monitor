import sqlite3
import time

import psutil

import sruns_monitor.utils as utils


class Db:
    RUNS_TABLE = "runs"
    RUN_STATUS_NEW = "new"
    RUN_STATUS_COMPLETE = "complete"
    RUN_STATUS_RUNNING = "running"
    RUN_STATUS_NEEDS_TAR = "start_tar"
    RUN_STATUS_NEEDS_UPLOAD = "start_upload"
    TASKS_TABLE_NAME = "tasks"
    # tasks attribute names
    TASKS_NAME = "name"
    TASKS_PID = "pid"
    TASKS_TARFILE = "tarfile"
    TASKS_BUCKET_TARFILE = "upload_status"

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
                {tarfile} text,
                {upload_status} text);
            """.format(table=self.TASKS_TABLE_NAME,  
                       name=self.TASKS_NAME, 
                       pid=self.TASKS_PID,
                       tarfile=self.TASKS_TARFILE,
                       upload_status=self.TASKS_BUCKET_TARFILE)

        self.curs.execute(create_table_sql)

    def insert_run(self, name, pid, tarfile, upload_status):
        sql = """
              INSERT INTO {table}({name_attr},{pid_attr},{tarfile_attr},{upload_status_attr})
              VALUES({name},{pid},{tarfile},{upload_status})
              """.format(
                  table=self.TASKS_TABLE_NAME,
                  name_attr=self.TASKS_NAME,
                  pid_attr=self.TASKS_PID,
                  tarfile_attr=self.TASKS_TARFILE,
                  upload_status_attr=upload_status_attr,
                  name=name,
                  pid=pid,
                  tarfile=tarfile,
                  upload_stats=upload_status)
        self.curs.execute(sql)

    def update_run(self, name, payload):
        update_str = ""
        for attr in payload:
            update_str += "{key}={val},".format(attr, payload[attr])
        update_str.rstrip(",")
        sql = "UPDATE {table} SET {updates} WHERE name={name}".format(
            table=self.TASKS_TABLE_NAME, 
            updates=update_str,
            name=name)
        self.curs.execute(sql)
              
    def get_run(self, name):
        """
        Returns:
            `tuple`: A record whose name attribute has the supplied name exists. 
            `None`: No such record exists.
        """
        sql = "SELECT {name},{pid},{tarfile},{upload_status} FROM {table} WHERE {name}={input_name}".format(
            name=self.TASKS_NAME, 
            pid=self.TASKS_PID,
            tarfile=self.TASKS_TARFILE, 
            upload_status=self.TASKS_BUCKET_TARFILE, 
            table=self.TASKS_TABLE_NAME,
            input_name=name)

        res = self.curs.execute(sql).fetchone()
        if not res:
            return {}
        return {
            self.TASKS_NAME: res[0],
            self.TASKS_PID: res[1],
            self.TASKS_TARFILE: res[2],
            self.TASKS_BUCKET_TARFILE: res[3]
        }

    def delete_run(self, name):
        sql = "DELETE FROM {table} WHERE {name}={input_name}".format(
            table=self.TASKS_TABLE_NAME,
            name=self.TASKS_NAME, 
            input_name=name)
        self.curs.execute(sql)
