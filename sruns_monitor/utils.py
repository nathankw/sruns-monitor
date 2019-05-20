import logging
import os
import tarfile

import pdb

import sruns_monitor as srm

def get_logfile_name(tag):
    """
    Creates a name for a log file that is meant to be used in a call to
    ``logging.FileHandler``. The log file name will incldue the path to the log directory given
    by the ``sruns_monitor.LOG_DIR`` constant. The format of the file name is: 'log_$TAG.txt', where 
    $TAG is the value of the 'tag' argument. The log directory will be created if need be.

    Args:
        tag: `str`. A tag name to add to at the end of the log file name for clarity on the
            log file's purpose.
    """
    if not os.path.exists(srm.LOG_DIR):
        os.mkdir(srm.LOG_DIR)
    filename = "log_srm_" + tag + ".txt"
    filename = os.path.join(srm.LOG_DIR, filename)
    return filename

def add_file_handler(logger, level, tag):
    """
    Adds a ``logging.FileHandler`` handler to the specified ``logging`` instance that will log
    the messages it receives at the specified error level or greater.  The log file will be named
    as outlined in ``get_logfile_name``.

    Args:
        logger: The `logging.Logger` instance to add the `logging.FileHandler` to.
        level:  `int`. A logging level (i.e. given by one of the constants `logging.DEBUG`,
            `logging.INFO`, `logging.WARNING`, `logging.ERROR`, `logging.CRITICAL`).
        tag: `str`. A tag name to add to at the end of the log file name for clarity on the
            log file's purpose.
    """
    f_formatter = logging.Formatter('%(asctime)s:%(name)s:\t%(message)s')
    filename = get_logfile_name(tag)
    handler = logging.FileHandler(filename=filename, mode="a")
    handler.setLevel(level)
    handler.setFormatter(f_formatter)
    logger.addHandler(handler)

def tar(input_dir, tarball_name):
    """
    Creates a tar.gz tarball of the provided directory and returns the tarball's name.
    The tarball's name is the same as the input directory's name, but with a .tar.gz extension.

    Args:
        input_dir: `str`. Path to the directory to tar up.
        tarball_name: `str`. Name of the output tarball.

    Returns:
        `None`.
    """
    with tarfile.open(tarball_name, mode="w:gz") as tb:
        tb.add(name=input_dir, arcname=os.path.basename(input_dir))

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

if __name__ == "__main__":
    tar(os.getcwd())
