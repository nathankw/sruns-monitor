
###
# Nathaniel Watson
# nathanielwatson@stanfordhealthcare.org
# 2019-05-16
###

import json
from multiprocessing import Process, Queue
import os
import tarfile
import time

from google.cloud import storage

storage_client = storage.Client()

import sruns_monitor.utils as utils

class Monitor:
    """
    Requires a configuration file in JSON format with settings regarding what folder to monitor
    and which GCP bucket to upload tarred runs to.
    """

    #: The presence of any file in this array indicates that the Run directory is ready for
    #: downstream processing (i.e. the Illumina NovaSeq has finished writing to the folder).
    #: The sential file can vary by sequencing platform. For NovaSeq, can use CopyComplete.txt.
    SENTINAL_FILES = set(["CopyComplete.txt"])

    def __init__(self, conf_file):
        self.conf = json.load(open(conf_file))
        self.state = Queue() # Must pass in manually to Process constructors
        self.bucket = storage_client.get_bucket(self.conf.gcp_bucket.name)

    def scan(self):
        for rundir in os.listdir(self.conf.location):
            if not os.path.isdir(rundir):
                continue
            if set(os.listdir(rundir)).intersection(self.SENTINAL_FILES):
                # This is a completed run directory
                p = Process(target=self.p_tar_and_upload, args=(self.state, rundir))

    def child(self, state):
        try:
            time.sleep(0.01)
            print("Hello from: ", os.getpid())
            time.sleep(0.01)
            print(b)
        except Exception as e:
            print("error")
            state.put((os.getpid(), e))

    def p_tar_and_upload(self, state, rundir):
        pid = os.getpid()
        try:
            print(b)
            tarball = utils.tar(rundir)
            # Upload tarball to GCP bucket
            blob_name = "/".join(os.path.basename(rundir), os.path.basename(tarball))
            utils.upload_to_gcp(bucket=self.bucket, blob_name=blob_name, source_file=tarball)
            
        except Exception as e
            print("error")
            state.put((pid, e))
            raise
        pass

    def start(self):
        while True:
            self.scan()
            data = self.state.get(block=False)
            if data:
                pid = data[0]
                msg = data[1]
                print("Process {} exited with message '{}'.".format(pid, msg))
            time.sleep(m.conf.cycle_pause)

#m = Monitor(conf_file="conf.json")
#m.start()
