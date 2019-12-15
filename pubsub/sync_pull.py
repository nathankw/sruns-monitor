import argparse
import json
import time

from google.cloud import pubsub_v1
import google.api_core.exceptions

"""
howdy

pip install google-cloud-pubsub
GCP documentation for creating a notification configuration at https://cloud.google.com/storage/docs/pubsub-notifications.
GCP documentation for synchronous pull at https://cloud.google.com/pubsub/docs/pull#synchronous_pull.
"""

def get_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-p", "--project_id", required=True, help="The Google Cloud Project ID")
    parser.add_argument("-s", "--subscription_name", required=True, help="The Pub/Sub subscription name")
    return parser


def get_msg_data(rcv_msg):
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
    print(json.dumps(jdata, indent=4))

def main():
    parser = get_parser()
    args = parser.parse_args()
    project_id = args.project_id
    subscription_name = args.subscription_name

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)

    try:
        response = subscriber.pull(subscription_path, max_messages=1)
    except google.api_core.exceptions.DeadlineExceeded:
        print("Nothing for now!")
        return
#
    ack_ids = []
    for received_message in response.received_messages:
        # Get JSON form of data
        jdata = get_msg_data(received_message)
        # Query Firestore for the run metadata to grab the location in Google Storage of the raw run.
        ack_ids.append(received_message.ack_id)

    # Acknowledges the received messages so they will not be sent again.
    subscriber.acknowledge(subscription_path, ack_ids)

    print(
        "Received and acknowledged {} messages. Done.".format(
            len(response.received_messages)
        )
    )

if __name__ == "__main__":
    main()
