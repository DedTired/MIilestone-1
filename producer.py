from google.cloud import pubsub_v1
import csv
import glob     
import json
import time
import os 

# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];
# Configuration
project_id = "cloud-milestone-1"  # Replace with your GCP project ID
topic_name = "vehicle-tracking"  # Replace with the desired topic name
csv_file = "Labels.csv"

# Initialize Publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

print(f"Publishing messages to topic: {topic_path}")

# Read the CSV and publish messages
with open(csv_file, "r") as file:
    reader = csv.DictReader(file)  # Automatically uses the header row as keys
    for row in reader:
        # Serialize the row into a JSON string
        message = json.dumps(row).encode("utf-8")
        try:
            # Publish the message
            future = publisher.publish(topic_path, message)
            future.result()  # Ensure the message is successfully published
            print(f"Produced message: {row}")  # Log the message
        except Exception as e:
            print(f"Failed to produce message: {e}")
        time.sleep(0.5)  # Optional delay to simulate real-time production
