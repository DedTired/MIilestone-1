from google.cloud import pubsub_v1
import json
import glob
import os

# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];
# Configuration
project_id = "cloud-milestone-1"  # Replace with your GCP project ID
subscription_name = "vehicle-tracking-sub"  # Replace with the subscription name

# Initialize Subscriber
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)

def callback(message):
    try:
        # Deserialize the message data
        data = json.loads(message.data.decode("utf-8"))
        print(f"Consumed message: {data}")  # Log the message
        message.ack()  # Acknowledge the message
    except Exception as e:
        print(f"Failed to consume message: {e}")
        message.nack()  # Nack the message for reprocessing

print(f"Listening for messages on subscription: {subscription_path}")

# Subscribe to messages
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

# Keep the consumer running
try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    subscriber.close()
