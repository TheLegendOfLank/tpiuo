import os
import requests
from google.cloud import pubsub_v1
import json
import time

PROJECT_ID = "norse-geode-477321-v2"
TOPIC_ID = "reddit-topic"

# Initialize publisher with ADC
publisher = pubsub_v1.PublisherClient()
print(f"✅ Publisher initialized for project: {PROJECT_ID}")

topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# Reddit API configuration
url = "https://www.reddit.com/r/dataengineering/top.json?limit=10&t=all"
headers = {
    "User-Agent": "Mozilla/5.0 (compatible; DataEngineeringLab/1.0; +http://yourapp.com)"
}

print("🌐 Fetching posts from Reddit API...")

try:
    response = requests.get(url, headers=headers, timeout=10)
    print(f"📊 Response status: {response.status_code}")
    print(f"📊 Response preview: {response.text[:200]}...")  # Debug log

    # Check if response is valid
    if response.status_code != 200:
        print(f"❌ Reddit API error: {response.status_code}")
        print(f"❌ Response body: {response.text}")
        # Exit or handle error appropriately
        exit(1)

    # Try to parse JSON
    data = response.json()

except requests.exceptions.RequestException as e:
    print(f"❌ Network error fetching from Reddit: {e}")
    exit(1)
except json.JSONDecodeError as e:
    print(f"❌ Failed to parse JSON from Reddit API")
    print(f"❌ Response content type: {response.headers.get('content-type')}")
    print(f"❌ First 500 chars of response: {response.text[:500]}")
    exit(1)

# Continue if we got valid data
if "data" in data and "children" in data["data"]:
    posts = data["data"]["children"]
    print(f"✅ Found {len(posts)} posts to publish")

    for post in posts:
        try:
            message = json.dumps(post["data"]).encode("utf-8")
            future = publisher.publish(topic_path, message)
            future.result()  # Wait for publish to complete
            print(f"📤 Sent: {post['data']['title'][:50]}...")
        except Exception as e:
            print(f"❌ Failed to publish post: {e}")

    print("✅ All messages processed!")
else:
    print(f"❌ Unexpected data structure from Reddit API")
    print(f"❌ Data keys: {list(data.keys())}")

# Keep the job running (adjust as needed)
print("🔄 Job completed. Exiting...")
