Sequencing Runs Monitor 
***********************

A tool that archives new Illumina sequencing runs to Google Cloud Storage

Use case
========
You have one or more Illumina sequencers that are writing to a mounted filesystem such as NFS.
You need a way to detect when there is a new, completed sequencing run directory and then relocate
it to redundant storage. Downstream tools need to be able to know when a tarred run directory is
available to post-processing (i.e. demultiplexing, QC, read alignment, etc.). 

How it works
============
Sequencing Runs Monitor solves the aforementioned challenges through the use of Google Cloud Platform
services and by tracking workflow state. Sequencing runs are tarred with gzip compression and then
uploaded to Google Cloud Storage. Workflow state is tracked both locally via SQLite and in the 
NoSQL database Google Firestore for redundancy and to allow downstream clients to query sequencing
run records. 

Note: while you don't need to use a Google compute instance to run the monitor script, the documentation
here assumes that you are since it is the recommended way. That's due to the fact that the monitor 
must interact with certain GCP services, and hence must be running with proper Google credentials
(i.e. a service account). 

The monitor script is named  *launch_monitor.py*; when running it you must provide it with the path 
to a JSON configuration file, described in detail further below. You should set up your compute 
instance to run this script as a daemon service.  

The workflow is fitted into two tasks: the *tar task* and the *upload task*. When the monitor 
detects a new sequencing run, it executes the workflow in a child process. The workflow is smart 
enough to detect which task to begin with, thanks to the local SQLite database. This database has 
a record for each sequencing run and tracks which workflow tasks have been completed, and whether 
the workflow is running. 

Tar task
-----------
Creates a tarball with gzip compression. The process ID is stored in the local run record in the 
SQLite database.  

Upload task
-----------
Uploads the tarfile to a Google bucket. This task fetches the run record from the local database
to get the path to the local tarfile. 

The configuration file
======================
This is a small JSON file that lets the monitor know things such as which GCP bucket and Firestore
collection to use, for example.  The possible keys are:

  * `watchdir`: (Required) The directory to monitor for new sequencing runs.
  * `completed_runs_dir`: (Required) The directory to move a run directory to after it has completed the 
    workflow. At present, there isn't a means to clean out the completed runs directory, but that
    will come in a future release. 
  * `sqlite_db`: The name of the local SQLite database to use for tracking workflow state. 
    Defaults to *sruns.db* if not specified. 
  * `firestore_collection`: (Required) The name of the Google Firestore collection to use for 
    persistent workflow state that downstream tools can query. If it doesn't exist yet, it will be
    created.
  * `gcp_bucket_name`: (Required) The name of the Google Cloud Storage bucket to which tarred run
    directories will be uploaded.
  * `gcp_bucket_basedir`: The directory in `gcp_bucket_name` in which to store all uploaded files. 
    Defaults to the root directory. 
  * `cycle_pause_sec`: The number of seconds to wait in-between scans of `watchdir`. Defaults to 60.
  * `task_runtime_limit_sec`: The number of seconds a child process is allowed to run before
    being killed. This is meant to serve as a safety mechanism to prevent errant child processes
    from consuming resources in the event that this does happen due to unforeseen circumstances.
    An email notification will be sent out in this case to alert about the errant process
    and the sequencing run it was associated with. The number of seconds you set for this depends
    on several factors, such as run size and network speed. It is suggested to use two days (172800
    seconds) at least to be conservative. 

Workflow state
==============
The state of the workflow for a given run directory is tracked both locally in a SQLite database
as well as Google Firestore - a NoSQL database. Local state is tracked for the purpose of being
able to restart workflows if a child process ever crashes, or if the node goes down. Firestore is
used to enable downstream applications to query the collection (whose name is specified in your 
configuration file) to do their own post-processing as desired. For example, an external tool
could query the collection and ask if a given run is completed yet. Completed in this sense means
that the run was tarred and uploded to a Google bucket. Then, the tool could tell where the tarfile 
blob is located.

SQLite
------
There is a record for every sequencing run, which is stored in the *tasks* table - the only table.
The possible fields are:

  * `name`: The name of the sequencing run.
  * `pid`: The process ID of the workflow that is running or that already ran. 
  * `tarfile`: The path to the local tarfil that was generated by the tar task. 
  * `gcp_tarfile`: The blob object path in the Google bucket, stored as *$bucket_name/$blob_name*.
  
Firestore
---------
There is a record in the collection for each sequencing run. The possible fields are:

  * `name`: The name of the sequencing run. This mirrors the value of the same attribute in the
    analagous SQLite database record. 
  * `storage`: Bucket storage object path for the tarred run directory in the 
    form bucket_name/path/to/run.tar.gz
  * `workflow_status`: The overall status of the worklfow. Possible values are:

    * `new`
    * `starting`
    * `tarring`
    * `tarring_complete`
    * `uploading`
    * `uploading_complete`
    * `complete`
    * `not_running`

Installation and setup
======================
This works in later versions of Python 3 only::

  pip3 install sruns-monitor

It is recommended to start your compute instance (that will run the monitor) using a service account
with the following roles:

  * roles/storage.objectAdmin
  * roles/datastore.owner



