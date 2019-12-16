# -*- coding: utf-8 -*-

from email.message import EmailMessage
import json
import jsonschema
import os
import psutil
from smtplib import SMTP, SMTPException
import shutil
import tarfile
import time

import sruns_monitor as srm


def validate_conf(conf_file):
    """
    Ensures that the configuration file is valid according to the internal schema. Before
    validating, any keys that start with '#' will be removed from the JSON object.

    Args:
        conf_file: `str`. The JSON configuration file.
    """
    conf_fh = open(conf_file)
    jconf = json.load(conf_fh)
    conf_fh.close()
    # Remove any keys that have been commented out
    for key in list(jconf.keys()):
        if key.startswith("#"):
            jconf.pop(key)
    schema_fh = open(srm.CONF_SCHEMA)
    jschema = json.load(schema_fh)
    schema_fh.close()
    jsonschema.validate(jconf, jschema)
    return jconf


def tar(input_dir, tarball_name, compress=False):
    """
    Creates a tar.gz tarball of the provided directory and returns the tarball's name.
    The tarball's name is the same as the input directory's name, but with a .tar.gz extension.

    Args:
        input_dir: `str`. Path to the directory to tar up.
        tarball_name: `str`. Name of the output tarball.
        compress: `boolean`. True enables gzip compression. Not recommended with the latest types of
            Illumina runs, i.e. NovaSeq, since the files are mostly binary which isn't compressible.
            For example, I compressed a 428 GB NovaSeq run with 'tar -zcf' and the output was 422 GB.

    Returns:
        `None`.
    """
    mode = 'w'
    if compress:
        mode = "w:gz"
    with tarfile.open(tarball_name, mode=mode) as tb:
        tb.add(name=input_dir, arcname=os.path.basename(input_dir))

def extract(filename, where):
   """
   Extracts a tar file.

   Args:
       filename: `str`. Local filepath of the file to extract.
       where: `str`. The local directory path in which `filename` will be extracted. 
  
   """
   tf = tarfile.open(filename)
   tf.extractall(path=where)

def upload_to_gcp(bucket, blob_name, source_file):
    """
    Uploads a local file to GCP storage in the specified bucket.

    Args:
        bucket: `google.cloud.storage.bucket.Bucket` instance, which can be created like so::

            from google.cloud import storage
            storage_client = storage.Client()
            bucket = storage_client.get_bucket("my_bucket_name")

        blob_name: `str`. The name to give the uploaded file in the bucket.
        source_file: `str`. The name of the local file to upload.

    Returns:
        `None`.

    Raises:
        `FileNotFoundError`: source_file was not locally found.
    """
    blob = bucket.blob(blob_name)
    return blob.upload_from_filename(source_file)

def get_process(pid):
    """
    Args:
        pid: `int`. The process ID of a process.

    Returns:
        `psutil.Process`: There is a process that exists in the system process table.
        `None`: There is not a process that exists in the system process table.
    """
    try:
        process = psutil.Process(pid)
        return process
    except psutil.NoSuchProcess:
        return None

def running_too_long(process, limit_seconds=None):
    """
    Indicates whether a process has been running longer than a specified amount of time
    in seconds.

    Args:
       process: `psutil.Process` instance.
       limit_seconds: `int`. Number of seconds. If the process has
           been running longer than this amount of seconds, this function will return True.

    Returns:
        `boolean`.
    """
    if not limit_seconds:
        return False
    created_at = process.create_time() # Seconds since epoch
    process_age = (time.time() - created_at)
    if process_age > limit_seconds:
        return True
    return False

def get_file_age(filepath):
    """
    Returns the age of the file in seconds.
    """
    now = time.time() # seconds since epoch
    file_mtime = os.path.getmtime(filepath)
    return now - file_mtime


def get_time_since_ctime(filepath):
    """
    Gets the difference, in minutes, of the current time minus the specified file's inode change
    time (ctime, for metadata modification). This is useful for determinig how long a file as been
    copied over to NFS since - the mtime and atime timestamps are preservered from the copy source,
    but the ctime in this sense reveals when the file was copied to NFS since the file permissions
    changed at that point in time.

    Returns:
        `float`.
    """
    ctime = os.path.getctime(filepath)
    now_time = time.time() # seconds since the epoch
    diff_minutes = (now_time - ctime)/60.0
    return diff_minutes

def delete_directory_if_too_old(dirpath, age_seconds):
    """
    Removes the directory if it hasn't been modified  since the specified number of seconds.

    Returns:
        `boolean`: True means that the directory was removed.
    """
    if get_file_age(dirpath) >= age_seconds:
        shutil.rmtree(dirpath)
        return True
    return False

def send_mail(from_addr, to_addrs, subject, body, host):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg.set_content(body)
    with SMTP(host=host, timeout=5) as smtp:
        return smtp.send_message(msg=msg, from_addr=from_addr, to_addrs=to_addrs)
