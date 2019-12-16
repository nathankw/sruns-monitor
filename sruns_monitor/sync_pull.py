import argparse
import json
import time

import sruns_monitor as srm

from google.cloud import firestore
from google.cloud import pubsub_v1
from google.cloud import storage
import google.api_core.exceptions

"""
howdy

pip install google-cloud-pubsub
GCP documentation for creating a notification configuration at https://cloud.google.com/storage/docs/pubsub-notifications.
GCP documentation for synchronous pull at https://cloud.google.com/pubsub/docs/pull#synchronous_pull.
"""


class Poll:

    def __init__(self, project_id, subscription_name, conf_file):
        self.conf = utils.validate_conf(conf_file)   
        self.basedir = "demultiplexing"
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = subscriber.subscription_path(self.project_id, self.subscription_name)
        self.firestore_collection_name = self.conf.get(srm.C_FIRESTORE_COLLECTION)
        self.firestore_coll = firestore.Client().collection(self.firestore_collection_name)
        self.run_dir_bucket = self.get_bucket(self.conf[srm.C_GCP_BUCKET_NAME])

    def get_bucket(self, bucket_name)
        client = storage.Client()
        return bucket = client.get_bucket(self.bucket_name)

    def download(self, bucket, object_path, download_dir)
        """
        Downloads the specified object from the specified bucket in `download_dir`.

        Args:
            bucket: `google.cloud.storage.bucket.Bucket` instance (i.e. from `self.get_bucket`).
            object_path: `str`. The object path within `bucket` to download. 
            download_dir: `str`. Directory in which to download the file. 
        """
        blob = bucket.get_blob(object_path)
        filename = os.path.join(download_dir, os.path.basename(object_path))
        blob.download_to_filename(filename)
        return filename

    def get_firestore_document(self, run_name):
        """
        Args: 
            run_name: `str`. The name of the sequencing run at hand. Used to query Firestore for a
                document having the 'name' attribute set to this. 

        Returns: `google.cloud.firestore_v1.document.DocumentReference`
        """
        
        return self.firestore_coll.document(run_name)

    def get_msg_data(self, rcv_msg):
        """
        Args:
            rcv_msg: `google.cloud.pubsub_v1.types.ReceivedMessage`. 
    
        Example:
            At the heart of the received messages is the data that can be loaded as JSON, which will give
            us something like this:
    
            > jdata = json.loads(rcv_msg.message.data)
            > print(json.dumps(jdata), indent=4)
    
            {
                "kind": "storage#object",
                "id": "mysamplesheets/191022_A00737_0011_BHNLYYDSXX.csv/1576441248192471",
                "selfLink": "https://www.googleapis.com/storage/v1/b/mysamplesheets/o/191022_A00737_0011_BHNLYYDSXX.csv",
                "name": "191022_A00737_0011_BHNLYYDSXX.csv",
                "bucket": "mysamplesheets",
                "generation": "1576441248192471",
                "metageneration": "1",
                "contentType": "text/csv",
                "timeCreated": "2019-12-15T20:20:48.192Z",
                "updated": "2019-12-15T20:20:48.192Z",
                "storageClass": "STANDARD",
                "timeStorageClassUpdated": "2019-12-15T20:20:48.192Z",
                "size": "768",
                "md5Hash": "263ptiIm6ZYJ5u1KahDHzw==",
                "mediaLink": "https://www.googleapis.com/download/storage/v1/b/mysamplesheets/o/191022_A00737_0011_BHNLYYDSXX.csv?generation=1576441248192471&alt=media",
                "crc32c": "kgzmwA==",
                "etag": "CNev7qS9uOYCEAE="
            }
        """
        ack_id = rcv_msg.ack_id
        #: msg is a `google.cloud.pubsub_v1.types.PubsubMessage`
        msg = rcv_msg.message
        #: msg.data is a `bytes` object. 
        jdata = json.loads(msg.message.data)
        return jdata

    def pull(self):
        try:
            response = subscriber.pull(self.subscription_path, max_messages=1)
        except google.api_core.exceptions.DeadlineExceeded:
            print("Nothing for now!")
            return None

        ack_ids = []
        for received_message in response.received_messages:
            # Get JSON form of data
            jdata = self.get_msg_data(received_message)
            run_name = jdata[srm.FIRESTORE_ATTR_RUN_NAME].split(".")[0]
            # Query Firestore for the run metadata to grab the location in Google Storage of the raw run.
            print(f"Querying Firestore for a document with name '{run_name}'"
            #: doc is a `google.cloud.firestore_v1.document.DocumentReference` object.
            docref = self.get_firestore_document(run_name=run_name)
            doc = docref.get().to_dict() # dict
            if not doc:
                msg = f"No Firestore document exists for run '{run_name}'."
                raise Exception(msg)
            # Get path to raw run data in Google Storage. Has bucket name as prefix, i.e.
            # mybucket/path/to/obj
            raw_run_path = doc.get(srm.FIRESTORE_ATTR_STORAGE)
            if not raw_run_path:
                msg = f"Firestore document '{run_name}' doesn't have the storeage path attribute '{srm.FIRESTORE_ATTR_STORAGE}' set!"
                msg += f" Did the sequencing run finish uploading to Google Storeage yet?"
                raise Exception(msg)
            # Strip off bucket name
            raw_run_path = raw_run_path.split("/", 1)[1]
            samplesheet_pubsub_data = jdata.get(srm.FIRESTORE_ATTR_SS_PUBSUB_DATA)
            if not samplesheet_pubsub_data:
                docref.set({srm.samplesheet_pubsub_data: jdata})
            else:
                # Check if metageneration number is the same. 
                # If same, then we got a duplicate message from pubsub and can ignore. But if
                # different, then the SampleSheet was re-uploaded and we should process it again
                # (i.e. maybe the original SampleSheet had an incorrect barcode assignment). 
                prev_meta_gen = samplesheet_pubsub_data["metageneration"]
                if meta_gen == jdata["metageneration"]:
                    # duplicate message sent. Rare, but possible occurrence. 
                    return
                else:
                    # Overwrite previous value for srm.FIRESTORE_ATTR_SS_PUBSUB_DATA
                    docref.set({srm.samplesheet_pubsub_data: jdata})
            # Download raw run data
            download_dir = os.path.join(self.basedir, run_name, jdata["metageneration"])
            raw_data_path = self.download(bucket=self.run_dir_bucket, object_path=raw_run_path, download_dir=download_dir)
            ss_bucket = self.get_bucket(jdata["bucket"])
            samplesheet_path = self.download(bucket=ss_bucket, object_path=jdata["name"], download_dir=download_dir)
            # extract tarball

            ack_ids.append(received_message.ack_id)
    
        # Acknowledges the received messages so they will not be sent again.
        subscriber.acknowledge(subscription_path, ack_ids)
    
        print(
            "Received and acknowledged {} messages. Done.".format(
                len(response.received_messages)
            )
        )


def get_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-c", "--conf-file", required=True, help="""
        Path to JSON configuration file.  This is the same one that the monitor uses.
    """)
    parser.add_argument("-p", "--project_id", required=True, help="The Google Cloud Project ID")
    parser.add_argument("-s", "--subscription_name", required=True, help="The Pub/Sub subscription name")
    return parser

def main():
    parser = get_parser()
    args = parser.parse_args()
    project_id = args.project_id
    subscription_name = args.subscription_name
    conf_file = args.conf_file
    poll = Poll(project_id=project_id, subscription_name=subscription_name, conf_file=conf_file)
    poll.pull()

if __name__ == "__main__":
    main()
