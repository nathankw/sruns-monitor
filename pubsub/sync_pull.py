import argparse
import time

from google.cloud import pubsub_v1

"""
howdy

pip install google-cloud-pubsub
"""

def get_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-p", "--project_id", required=True, help="The Google Cloud Project ID")
    parser.add_argument("-s", "--subscription_name", required=True, help="The Pub/Sub subscription name")
    return parser

def callback(message):
    print("Received message: {}".format(message.data))
    if message.attributes:
        print("Attributes:")
        for key in message.attributes:
            value = message.attributes.get(key)
            print("{}: {}".format(key, value))
    message.ack()

def main():
    parser = get_parser()
    args = parser.parse_args()
    project_id = args.project_id
    subscription_name = args.subscription_name

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)

    NUM_MESSAGES = 3

#    # The subscriber pulls a specific number of messages.
    response = subscriber.pull(subscription_path, max_messages=NUM_MESSAGES)
#
    ack_ids = []
    for received_message in response.received_messages:
        print("Received: {}".format(received_message.message.data))
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
