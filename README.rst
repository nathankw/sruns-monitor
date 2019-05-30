#######################
Sequencing Runs Monitor 
#######################

--------------------------------------------------------------------------
A tool that archives new Illumina sequencing runs to Google Cloud Storage
--------------------------------------------------------------------------

Use case
========
You have one or more Illumina sequencers that are writing to a mounted filesystem such as NFS.
You need a way to detect when there is a new, completed sequencing run directory and then relocate
it to redundant storage. This tool provides such a service, using Google Coud Storage buckets 
as the redundant storage solution. Workflow state is tracked in Google Firestore - a NoSQL database.

Installation
============
This works in Python3 only.

*pip3 install sruns-monitor*

How it works
============
Note: while you don't need to use a Google compute instance to run the monitor script, the documentation
here assumes that you are since it is the recommended way. That's due to the fact that the monitor 
must interact with certain GCP services, and hence must be running with proper Google credentials
(i.e. service account). 

Run the script launch_monitor.py, providing it with the path to a JSON configuration file, described
in detail below. You should set up your compute instance to run this script as a daemon service. 
When a new run is detected, the monitor will begin the tar task to tar the run directory with gzip 
compression in a child process. Once the tarring is complete, the upload task will run and upload
the tar file to the GCP bucket of your choice. The uploaded tarred run directory is placed into a 
directory in the bucket that is named after the sequencing run itself. 

Workflow state
--------------
The state of the workflow for a given run directory is tracked both locally in a SQLite database
as well as Google Firestore - a NoSQL database. Local state is tracked for the purpose of being
able to restart workflows if a child process ever crashes, or if the node goes down. Firestore is
used to enable downstream applications to query the collection (whose name is specified in your 
configuration file) to do their own post-processing as desired. For example, an external tool
could query the collection and ask if a given run is completed yet. Completed in this sense means
that the run was tarred and uploded to GCP. Then, the tool could tell where the tarfile blob is
located.

The configuration file
======================
This is a small JSON file that lets the monitor know things such as which GCP bucket and Firestore
collection to use, for example.  The possible keys are:

  * `watchdir`: (Required) The directory to monitor for new sequencing runs.
  * `completed_runs_dir`: (Required) The directory to move a run directory to after it has completed the 
    workflow. At present, there isn't a means to clean out the completed runs directory, but that
    will come in a future release. 
  * `sqlite_db`: The name of the local SQLite database to use for tracking local workflow state. 
    Defaults to sruns.db if not specified. 
  * `firestore_collection`: (Required) The name of the Google Firestore collection to use for persistant workflow
    state. If it doesn't exist yet, it will be created. 
  * `gcp_bucket_name`: (Required) The name of the Google Cloud Storage bucket to which tarred run directories
    will be uploaded.
  * `gcp_bucket_basedir`: The directory in `gcp_bucket_name` in which to store all uploaded files. 
    Defaults to the root directory. 
  * `cycle_pause_sec`: The number of seconds to wait in-between scans of `watchdir`. Defaults to 60.
  * `task_runtime_limit_sec`: The number of seconds a child process is allowed to run for before
    being killed. This is meant to serve as a safety mechanism to prevent erronous child processes
    from consuming resources in the event that this does happen due to unforeseen circumstances.
    An email notification will be sent out in such circumstances to alert about the errant process
    and the sequencing run it was associated with. The number of seconds you set for this depends
    on several factors, such as run size and network speed. It is suggested to use two days (172800
    seconds) at least to be conservative. 

