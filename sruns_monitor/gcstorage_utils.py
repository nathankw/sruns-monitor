# -*- coding: utf-8 -*-

import os
import logging
import subprocess

from google.cloud import storage

import sruns_monitor as srm
from sruns_monitor import utils


logger = logging.getLogger(__name__)

def get_bucket(bucket_name):
    client = storage.Client()
    return client.bucket(bucket_name)

def download(bucket, object_path, download_dir):
    """
    Downloads the specified object from the specified bucket in `download_dir`.
    The file will be downloaded in the directory specified by the `download_dir` argument.

    Args:
        bucket: `google.cloud.storage.bucket.Bucket` instance (i.e. from `get_bucket()`).
        object_path: `str`. The object path within `bucket` to download.
        download_dir: `str`. Directory in which to download the file.

    Returns:
        `str`: The full path of the downloaded bucket object.
    """
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    blob = bucket.get_blob(object_path)
    filename = os.path.join(download_dir, os.path.basename(object_path))
    logger.info(f"Downloading gs://{bucket.name}/{object_path} to {download_dir}")
    blob.download_to_filename(filename)
    return filename

def upload_file(bucket, filepath, object_path):
    """
    Uploads the specified file to the specified bucket at the specified location.

    Args:
        bucket: `google.cloud.storage.bucket.Bucket` instance (i.e. from `get_bucket()`).
        filepath: `str`. Local path to a file to upload into the bucket.
        object_path: `str`. The object path to upload to.
    """
    blob = bucket.blob(object_path)
    logger.info(f"Uploading file '{filepath}' to gs://{bucket.name}/{object_path}")
    blob.upload_from_filename(filepath)


def upload_folder(bucket, folder, bucket_path):
    """
    Uploads the specified file to the specified bucket at the specified location.

    Args:
        bucket: `google.cloud.storage.bucket.Bucket` instance (i.e. from `get_bucket()`).
        folder: `str`. Local path to a folder to upload into the bucket.
        bucket_path: `str`. The object path to upload to.
    """
    bucket_path = bucket_path.strip("/")
    bucket_base_path = bucket_path + "/" + os.path.basename(folder) 
    for root,dirnames,filenames in os.walk(folder):
        for f in filenames:
            filepath = os.path.join(root, f)
            object_path = bucket_base_path + "/" + filepath.split(folder)[-1].strip("/")
            upload_file(bucket, filepath, object_path)
    
