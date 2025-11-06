import json
from google.cloud import pubsub_v1
from google.oauth2 import service_account

PROJECT_ID = "norse-geode-477321-v2"
SUBSCRIPTION_ID = "reddit-topic-sub"

# Load credentials properly
credentials_path = r"private-key.json"
creds = service_account.Credentials.from_service_account_file(credentials_path)

# Initialize subscriber
subscriber = pubsub_v1.SubscriberClient(credentials=creds)
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

def callback(message):
    try:
        payload = message.data.decode("utf-8").strip()
        if not payload:
            print("⚠️ Empty message received, skipping.")
            message.ack()
            return

        data = json.loads(payload)
        print(f"✅ Received post: {data.get('title', '(no title)')}")
        message.ack()

    except json.JSONDecodeError:
        print(f"⚠️ Non-JSON message received: {message.data}")
        message.ack()
    except Exception as e:
        print(f"❌ Error processing message: {e}")
        message.nack()


print(f"Listening for messages on {subscription_path}...\n")
streaming_pull = subscriber.subscribe(subscription_path, callback=callback)

# Keep program running
try:
    streaming_pull.result()
except KeyboardInterrupt:
    streaming_pull.cancel()
