# -*- coding: utf-8 -*-

###
# Nathaniel Watson
# nathanielwatson@stanfordhealthcare.org
# 2019-05-16
###

import json
import jsonschema
import logging
from multiprocessing import Process, Queue
import os
import signal
import sys
import tarfile
import time

from google.cloud import storage, firestore

import psutil

import sruns_monitor as srm
import sruns_monitor.utils as utils
from sruns_monitor.sqlite_utils import Db

class ConfigException(Exception):
    pass

class MissingTarfile(Exception):
    pass

class Monitor:
    """
    Requires a configuration file in JSON format with settings regarding what folder to monitor
    and which GCP bucket to upload tarred runs to.
    """

    #: The presence of any file in this array indicates that the Run directory is ready for
    #: downstream processing (i.e. the Illumina NovaSeq has finished writing to the folder).
    #: The sential file can vary by sequencing platform. For NovaSeq, can use CopyComplete.txt.
    SENTINAL_FILES = set(["CopyComplete.txt"])

    #: Status value for a new sequencing run. 
    RUN_STATUS_NEW = "new"
    #: Status value for a sequencing run that has completed the workflow. 
    RUN_STATUS_COMPLETE = "complete"
    #: Status value for a sequencing run that is running in the workflow. 
    RUN_STATUS_RUNNING = "running"
    #: Status value for a sequencing run that is has partially gone through the workflow, and the
    #: workflow is no longer running. For example, the tarfile task ran but the upload to GCP 
    #: task didn't because maybe it failed for some reason. 
    RUN_STATUS_NOT_RUNNING = "not_running"

    def __init__(self, conf_file):
        self.conf = self._validate_conf(conf_file)
        self.gcp_storage_client = storage.Client()
        self.firestore_coll = firestore.Client().collection(self.conf["firestore_collection"])
        self.watchdir = self.conf[srm.C_WATCHDIR]
        if not os.path.exists(self.watchdir):
            raise ConfigException("'watchdir' is a required property and the referenced directory must exist.".format(self.watchdir))
        self.completed_runs_dir = self.conf.get(srm.C_COMPLETED_RUNS_DIR)
        if not os.path.exists(self.completed_runs_dir):
            os.mkdir(self.completed_runs_dir)
        #: The number of seconds to wait between run directory scans, with a default of 60.
        self.cycle_pause_sec = self.conf.get(srm.C_CYCLE_PAUSE_SEC, 60)
        #: The number of seconds that a child process running the workflow is allowed to run, after
        #: which the process will be killed. A value of 0 indicates that such a time limit will not
        #: be observed.
        self.process_runtime_limit_sec = self.conf.get(srm.C_TASK_RUNTIME_LIMIT_SEC, None)
        #: A `multiprocessing.Queue` instance that a child process will write to in the event that
        #: an Exception is to occur within that process prior to re-raising the Exception and exiting.
        #: The main process will check this queue in each scan iteration to report any child processes
        #: that have failed by means of logging and email notification.
        self.state = Queue() # Must pass in manually to multiprocessing.Process constructors.
        #: The GCP Storage bucket name in which tarred run directories will be stored.
        self.bucket_name = self.conf[srm.C_GCP_BUCKET_NAME]
        #: A `google.cloud.storage.bucket.Bucket` instance.
        self.bucket = self.gcp_storage_client.get_bucket(self.bucket_name)
        #: The directory in `self.bucket` in which to store tarred run directories. If not provided,
        #: defaults to the root level directory.
        self.bucket_basedir = self.conf.get(srm.C_GCP_BUCKET_BASEDIR, "/")
        #signal.signal(signal.SIGTERM, Monitor._cleanup)
        signal.signal(signal.SIGINT, Monitor._cleanup)
        signal.signal(signal.SIGTERM, Monitor._cleanup)
        #: The local sqlite database in which to store workflow status for a given run. If not provided,
        #: defaults to 'sruns.db'. See `sruns_monitor.sqlite_utils.Db` for more details on the
        #: structure of records in this database.
        self.db = Db(self.conf.get(srm.C_SQLITE_DB, "sruns.db"))

        #: A reference to the `debug` logging instance that was created earlier in ``sruns_monitor.debug_logger``.
        #: Here, a file handler is being added to it for logging all messages sent to it. 
        #: The log file resides locally within the directory specified by the constant
        #: ``sruns_monitor.LOG_DIR``.
        self.debug_logger = logging.getLogger(srm.DEBUG_LOGGER_NAME)
        # Add debug file handler to debug_logger:
        utils.add_file_handler(logger=self.debug_logger, level=logging.DEBUG, tag="debug")

        #: A reference to the `error` logging instance that was created earlier in ``sruns_monitor.error_logger``.
        #: Here, a file handler is being added to it for logging terse error messages.
        #: The log file resides locally within the directory specified by the constant
        #: ``sruns_monitor.LOG_DIR``. Accepts messages >= ``logging.ERROR``.
        self.error_logger = logging.getLogger(srm.ERROR_LOGGER_NAME)
        utils.add_file_handler(logger=self.error_logger, level=logging.ERROR, tag="error")

    def _validate_conf(self, conf_file):
        """
        Args:
            conf_file: `str`. The JSON configuration file.
        """
        jconf = json.load(open(conf_file))
        jschema = json.load(open(srm.CONF_SCHEMA))
        jsonschema.validate(jconf, jschema)
        return jconf


    @staticmethod
    def _cleanup(signum, frame):
        """
        Terminate all child processes. Normally this is called when a SIGTERM is caught

        Args:
            signum: Don't call explicitly. Only used internally when this method is serving as a
                handler for a specific type of signal in the funtion `signal.signal`.
            frame: Don't call explicitly. Only used internally when this method is serving as a
                handler for a specific type of signal in the funtion `signal.signal`.
        """
        self.error_logger.error("Caught signal {signum}. Preparing for shutdown.".format(signum))
        # email notification
        pid = os.getpid()
        child_processes = psutil.Process().children()
        # Kill child processes by sending a SIGKILL.
        [c.kill() for c in child_processes] # equiv. to os.kill(pid, signal.SIGKILL) on UNIX.
        sys.exit(128 + signum)

    def get_rundir_path(self, run_name):
        return os.path.join(self.watchdir, run_name)

    def _workflow(self, state, run_name):
        """
        Runs the workflow. Knows which stages to run, which is useful if the workflow needs to
        be rerun from a particular point.

        This method is meant to serve as the value of the `target` parameter in a call to
        `multiprocessing.Process`, and is not meant to be called directly by users of this library.

        Args:
            state: `multiprocessing.Queue` instance.
            run_name: `str`. The name of a sequencing run.
        """
        self.db.update_run(name=run_name, payload={self.db.TASKS_PID: os.getpid()})
        rec = self.db.get_run(run_name)
        if not rec[self.db.TASKS_TARFILE]:
            self.task_tar(state=state, run_name=run_name)
        if not rec[self.db.TASKS_GCP_TARFILE]:
            self.task_upload(state=state, run_name=run_name)

    def task_tar(self, state,  run_name):
        """
        Creates a gzip tarfile of the run directory. The tarfile will be created in the directory
        being watched (`self.watchdir`) and named the same as the `run_name` parameter, but with
        a .tar.gz suffix.

        Once tarring is complete, the database record is updated such that the attribute
        `sqlite_utils.Db.TASKS_TARFILE` is set to the path of the tarfile.

        Args:
            state: `multiprocessing.Queue` instance.
            run_name: `str`. The name of a sequencing run.
        """
        rundir_path = self.get_rundir_path(run_name)
        tarball_name = rundir_path + ".tar.gz"
        try:
            tarball = utils.tar(rundir_path, tarball_name)
            self.db.update_run(name=un_name, payload={self.db.TASKS_TARFILE: tarball_name})
        except Exception as e:
            state.put((os.getpid(), e))
            # Let child process terminate as it would have so this error is spit out into
            # any potential downstream loggers as well. This does not effect the main thread.
            raise

    def task_upload(self, state, run_name):
        """
        Uploads the tarred run dirctory to GCP Storage in the directory specified by `self.bucket_basedir`.
        The blob is named as $basedir/run_name/tarfile, where run_name is the squencing run name,
        and tarfile is the name of the tarfile produced by `self.task_tar`.

        Once uploading is complete, the local database record is updated such that the attribute
        `sqlite_utils.Db.TASKS_GCP_TARFILE` is set to the location of the blob as a string value
        formatted as '$bucket_name/blob_path'.

        Args:
            state: `multiprocessing.Queue` instance.
            run_name: `str`. The name of a sequencing run.

        Raises:
            `MissingTarfile`: There isn't a tarfile for this run (based on the record information
            in self.db.
        """
        try:
            rec = self.db.get_run(run_name)
            tarfile = rec[self.db.TASKS_TARFILE]
            if not tarfile:
                raise MissingTarfile("Run {} does not have a tarfile.".format(run_name))
            # Upload tarfile to GCP bucket
            blob_name = "/".join(self.bucket_basedir, run_name, os.path.basename(tarfile))
            utils.upload_to_gcp(bucket=self.bucket, blob_name=blob_name, source_file=tarfile)
            self.db.update_run(
                name=run_name,
                payload={self.db.TASKS_GCP_TARFILE: "/".join(self.bucket_name, blob_name)})
        except Exception as e:
            state.put((pid, e))
            # Let child process terminate as it would have so this error is spit out into
            # any potential downstream loggers as well. This does not effect the main thread.
            raise

    def get_run_status(self, run_name):
        """
        Determines the state of the workflow for a given run based on the run record in the
        database. This method has a potential side effect: If the workflow is running and the child
        process encapsulating it has been running longer than a configurable number of seconds, then
        this method will kill that child process prior to returning the run status.

        Args:
            run_name: `str`. The name of a sequencing run.

        Returns:
            `str`. One of the RUN_STATUS_* constants defined in the class `sruns_monitor.sqlite_utils.Db`.
        """
        # Check for record in database
        rec = self.db.get_run(run_name)
        if not rec:
            return self.RUN_STATUS_NEW
        elif rec[self.db.TASKS_TARFILE] and rec[self.db.TASKS_GCP_TARFILE]:
            return self.RUN_STATUS_COMPLETE
        pid = rec[self.db.TASKS_PID]
        if not pid:
            return self.RUN_STATUS_NOT_RUNNING
        # Check if running
        try:
            process = psutil.Process(pid)
            if self.running_too_long(process):
                process.kill()
                # Send email
            # If the process was running too long and got killed, then the next iteration of the
            # monitor will see that the pid isn't running and restart the workflow. To be safe then,
            # return a running status because we can't be sure that the kill signal worked just yet.
            return self.RUN_STATUS_RUNNING
        except psutil.NoSuchProcess:
            return self.RUN_STATUS_NOT_RUNNING

    def running_too_long(self, process):
        """
        Clears the run record's pid attr if there isn't a running process with that pid. That way,
        the workflow can be restarted for the run during the next iteration of the monitor.

        Otherwise, if there is a running process, kills the process if it has been running for more
        than a configurable amount of time, and sends out an email notification.

        Args:
           process: `psutil.Process` instance.

        Returns:
            `boolean`.
        """
        # Add check to make sure process hasn't been running for more than a configured
        # amount of time.
        if not self.process_runtime_limit_sec:
            return False
        created_at = process.create_time() # Seconds since epoch
        process_age = (time.time() - created_at)
        if process_age > self.process_runtime_limit_sec:
            return True
        return False

    def archive_run(self, run_name):
        """
        Moves the run directory to the completed runs directory.
        """
        from_path = self.get_rundir_path(run_name)
        to_path = os.path.join(self.completed_runs_dir, run_name)
        self.debug_logger.debug("Moving run {run} to completed runs location {loc}.".format(run=run_name, loc=to_path))
        os.rename(from_path, to_path)

    def process_new_run(self, run_name):
        """
        Create a new record into the local sqlite db as well as the Firestore db. 
        """
        self.db.insert_run(name=run_name)
        # Create Firestore document
        firestore_payload = {
            srm.FIRESTORE_ATTR_WF_STATUS: self.RUN_STATUS_RUNNING
        }
        self.firestore_coll.document(run_name).set(firestore_payload)
        self.run_workflow(run_name)

    def process_completed_run(self, run_name):
        """
        Moves the run directory to the completed runs directory location that is defined 
        by `sruns_monitor.C_COMPLETED_RUNS_DIR`. 

        Updates Firestore to set 

            * the GCP storage attribute (identified by the variable `sruns_monitor.FIRESTORE_ATTR_STORAGE`)
              to the location of the gzip tarfile of the run directory in GCP bucket storage. 
            * the workflow status attribute (identified by the variable `sruns_monitor.FIRESTORE_ATTR_WF_STATUS`.
              to completed. 

        """
        rec = self.db.get_run(run_name)
        self.archive_run(run_name)
        # Update Firestore record
        firestore_payload = {
            srm.FIRESTORE_ATTR_WF_STATUS: self.RUN_STATUS_COMPLETE,
            srm.FIRESTORE_ATTR_STORAGE: rec[self.db.TASKS_GCP_TARFILE]
        }
        self.firestore_coll.document(run_name).update(firestore_payload)

    def run_workflow(self, run_name):
        p = Process(target=self._workflow, args=(self.state, run_name))
        p.start()

    def scan(self):
        """
        Finds all sequencing run in `self.watchdir` that are finished sequencing.
        """
        run_names = []
        for run_name in os.listdir(self.watchdir):
            rundir_path = self.get_rundir_path(run_name)
            if not os.path.isdir(rundir_path):
                continue
            if set(os.listdir(rundir_path)).intersection(self.SENTINAL_FILES):
                # This is a completed run directory
                run_names.append(run_name)
        return run_names

    def process_rundirs(self, run_names):
        """
        For each sequencing run name, checks it's status with regard to the workflow and initiates
        any remaining steps, i.e. restart, cleanup, ...
        """
        for run_name in run_names:
            run_status = self.db.get_run_status(run_name)
            if run_status == self.RUN_STATUS_NEW:
                self.process_new_run(run_name)
            elif run_status == self.RUN_STATUS_COMPLETE:
                self.process_completed_run(run_name)
            elif run_status == self.RUN_STATUS_RUNNING:
                pass
            elif run_status == self.RUN_STATUS_NOT_RUNNING:
                self.run_workflow(run_name)

    def start(self):
        try:
            while True:
                # Remove any zombie processes
                # Curious why or how this works? See book Programming Python, 4th ed. section
                # "Killing the zombies: Don't fear the reaper!".
                os.waitpid(0, os.WNOHANG)
                finished_rundirs = self.scan()
                self.process_rundirs(run_names=finished_rundirs)
                data = self.state.get(block=False)
                if data:
                    pid = data[0]
                    msg = data[1]
                    self.error_logger.error("Process {} exited with message '{}'.".format(pid, msg))
                    # Email notification
                time.sleep(self.cycle_pause_sec)
        except Exception as e:
            # Email notification
            self.error_logger.error("Main process Exception: {}".format(e))
            raise

### Example
# m = Monitor(conf_file="my_conf_file.json")
# m.start()
###
