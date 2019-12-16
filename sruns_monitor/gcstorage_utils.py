# -*- coding: utf-8 -*-

import os
import logging

from google.cloud import storage

import sruns_monitor as srm


logger = logging.getLogger("SampleSheetSubscriber")

def get_bucket(bucket_name):
    client = storage.Client()
    return client.get_bucket(bucket_name)

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
