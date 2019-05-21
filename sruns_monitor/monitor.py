
###
# Nathaniel Watson
# nathanielwatson@stanfordhealthcare.org
# 2019-05-16
###

import json
import logging
from multiprocessing import Process, Queue
import os
import signal
import sys
import tarfile
import time

from google.cloud import storage
import psutil

import sruns_monitor as srm
import sruns_monitor.utils as utils
from sruns_monitor.sqlite_utils import Db

storage_client = storage.Client()

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
    #: File that contains the name of currnet process ID.

    def __init__(self, conf_file):
        self.conf = json.load(open(conf_file))
        self.state = Queue() # Must pass in manually to Process constructors
        self.bucket = storage_client.get_bucket(self.conf["gcp_bucket"]["name"])
        #signal.signal(signal.SIGTERM, Monitor._cleanup)
        signal.signal(signal.SIGINT, Monitor._cleanup)
        signal.signal(signal.SIGTERM, Monitor._cleanup)
        self.db = Db(self.conf["sqlite_db"])

        #: A reference to the `debug` logging instance that was created earlier in ``sruns_monitor.debug_logger``.
        #: This class adds a file handler, such that all messages sent to it are logged to this
        #: file in addition to STDOUT.
        self.debug_logger = logging.getLogger(srm.DEBUG_LOGGER_NAME)
        # Add debug file handler to debug_logger:
        utils.add_file_handler(logger=self.debug_logger, level=logging.DEBUG, tag="debug")

        #: A ``logging`` instance with a file handler for logging terse error messages.
        #: The log file resides locally within the directory specified by the constant
        #: ``connection.LOG_DIR``. Accepts messages >= ``logging.ERROR``.
        self.error_logger = logging.getLogger(srm.ERROR_LOGGER_NAME)
        utils.add_file_handler(logger=self.error_logger, level=logging.ERROR, tag="error")
        self.db = Db(self.conf["sqlite_db"])

    @staticmethod
    def _cleanup(signum=None, frame=None):
        """
        Terminate all child processes. Normally this is called when a SIGTERM is caught

        Args:
            signum: Don't call explicitly. Only used internally when this method is serving as a
                handler for a specific type of signal in the funtion `signal.signal`. 
            frame: Don't call explicitly. Only used internally when this method is serving as a
                handler for a specific type of signal in the funtion `signal.signal`. 
        """
        if signum:
            self.error_logger.error("Caught signal {signum}. Preparing for shutdown.".format(signum))
            # email notification
        pid = os.getpid()
        child_processes = psutil.Process().children()
        [c.kill() for c in child_processes]
        sys.exit(128 + signum)

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
        rec = self.db.get_run(run_name)
        payload = {}
        payload[self.db.TASKS_PID] = os.getpid() # Child pid
        self.db.update_run(name=run_name, payload=payload)
        if not rec[self.db.TASKS_TARFILE]: 
            self.task_tar(state=state, run_name=run_name)
        if not rec[self.db.TASKS_GCP_TARFILE]:        
            self.task_upload(state=state, run_name=run_name)

    def get_rundir_path(self, run_name):
        return os.path.join(self.conf["location"], run_name)

    def check_task_to_run(self, rec):
        if not rec[self.db.TASKS_TARFILE]:
            return self.db.RUN_STATUS_NEEDS_TAR
        elif not rec[self.db.TASKS_GCP_TARFILE]:
            return self.db.RUN_STATUS_NEEDS_UPLOAD
        else:
            return None

    def task_tar(self, state,  run_name):
        rundir_path = self.get_rundir_path(run_name)
        tarball_name = rundir_path + ".tar.gz"
        try:
            self.db.update_run(name=run_name, payload={self.Db.TASKS_PID: os.getpid())
            tarball = utils.tar(rundir_path, tarball_name)
            self.db.update_run(name=run_name, payload={self.Db.TASKS_TARFILE: tarball_name})
        except Exception as e:
            state.put((pid, e))
            # Let child process terminate as it would have so this error is spit out into
            # any potential downstream loggers as well. This does not effect the main thread.
            raise

    def task_upload(self, state, run_name):
        """
        Uploads the tarred run dirctory to GCP Storage. The blob is named as /run_name/tarfile,
        where run_name is the squencing run name, and tarfile is the name of the tarfile.

        Raises:
            `MissingTarfile`: There isn't a tarfile for this run (based on the record information
            in self.Db.
        """
        try:
            rec = self.db.get_run(run_name)
            tarfile = rec[self.Db.TASKS_TARFILE]
            if not tarfile:
                raise MissingTarfile("Run {} does not have a tarfile.".format(run_name))
            # Upload tarfile to GCP bucket
            blob_name = "/".join(run_name, os.path.basename(tarfile))
            utils.upload_to_gcp(bucket=self.bucket, blob_name=blob_name, source_file=tarfile)
            self.db.update_run(name=run_name, payload={self.Db.TASKS_GCP_TARFILE: blob_name})
        except Exception as e:
            state.put((pid, e))
            # Let child process terminate as it would have so this error is spit out into
            # any potential downstream loggers as well. This does not effect the main thread.
            raise

    def test(self):
        print(__package__)
        print("Starting as ", os.getpid())
        while True:
            time.sleep(10)

    def get_run_status(self, name):
        """
        Determines the state of the workflow for a given run based on the run record in the
        database.

        Returns:
            `str`. One of the RUN_STATUS_* constants defined in the class `sruns_monitor.sqlite_utils.Db`.
        """
        # Check for record in database
        rec = self.db.get_run(name)
        if not rec:
            return self.db.RUN_STATUS_NEW
        elif rec[self.db.TASKS_TARFILE] and rec[self.db.TASKS_GCP_TARFILE]:
            return self.db.RUN_STATUS_COMPLETE
        pid = rec[self.db.TASKS_PID]
        if not pid:
            return self.db.RUN_STATUS_NOT_RUNNING
        # Check if running
        try:
            process = psutil.Process(pid)
            if self.running_too_long(process):
                process.kill() 
                # Send email
            # If the process was running too long and got killed, then the next iteration of the 
            # monitor will see that the pid isn't running and restart the workflow. To be safe then,
            # return a running status because we can't be sure that the kill signal worked just yet. 
            return self.db.RUN_STATUS_RUNNING
        except psutil.NoSuchProcess:
            return self.db.RUN_STATUS_NOT_RUNNING
        
        else:
            return self.db.RUN_STATUS_HAS_PID

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
        created_at = process.create_time() # Seconds since epoch
        process_age_hours = (time.time.now() - created_at)/3600
        if process_age_hours > self.conf["process_runtime_terminate"]:
            return True
        return False

    def scan(self, watch_dir):
        for run_name in os.listdir(wath_dir):
            if not os.path.isdir(run_name):
                continue
            rundir_path = self.get_rundir_path(run_name)
            if set(os.listdir(rundir_path)).intersection(self.SENTINAL_FILES):
                # This is a completed run directory
                run_status = self.db.get_run_status(run_name)
                if run_status == self.db.RUN_STATUS_NEW:
                    # Insert record into sqlite db
                    self.db.insert_run(name=run_name)
                    p = Process(target=self._workflow, args=(self.state, run_name))
                    p.start()
                elif run_status == self.db.RUN_STATUS_COMPLETE:
                    move_to_path = os.path.join(self.conf["completed_runs_dir"], run_name)
                    self.debug_logger.debug("Moving run {run} to completed runs location {loc}.".format(run=run_name, move_to_path))
                    os.rename(rundir_path, move_to_path)
                elif run_status == self.db.RUN_STATUS_RUNNING:
                    pass
                elif run_status == self.db.RUN_STATUS_NOT_RUNNING
                    p = Process(target=self._workflow, args=(self.state, run_name))
                    p.start()

    def start(self):
        # Make sure this wasn't restarted and left orphaned processes.
        self._cleanup()
        watch_dir = self.conf["location"] 
        try:
            while True:
                self.scan(watch_dir=watch_dir)
                data = self.state.get(block=False)
                if data:
                    pid = data[0]
                    msg = data[1]
                    self.errog_logger.error("Process {} exited with message '{}'.".format(pid, msg))
                    # Email notification
                time.sleep(m.conf["cycle_pause"])
        except Exception as e:
            pass

### Example
# m = Monitor(conf_file="my_conf_file.json")
# m.start()
###
