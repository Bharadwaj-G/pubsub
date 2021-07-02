
import os
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'sa_credentials.json'
subscription_id = "gcp-training-topic-sub"
project_id = "trainingproject-317506"
timeout = 100.0
f = open("output.txt", "w")
subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(project_id, subscription_id)

print("Received msgs")
def callback(message):
    print(f"Received {message}.")
    print(f"{message.data}.")
    data = (message.data).decode("utf-8")
    if data.strip():
        f.write(data)
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.



f.close()





