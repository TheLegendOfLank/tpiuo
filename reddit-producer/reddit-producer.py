import os
import requests
from google.cloud import pubsub_v1
import json
import time

PROJECT_ID = "norse-geode-477321-v2"
TOPIC_ID = "reddit-topic"

# No explicit credentials - uses Application Default Credentials (ADC)
publisher = pubsub_v1.PublisherClient()
print(f"Publisher initialized: {publisher}")

topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

url = "https://www.reddit.com/r/dataengineering/top.json?limit=10&t=all"
headers = {"User-Agent": "dataengineering-lab-script/1.0"}

print("Fetching posts from Reddit API...")
response = requests.get(url, headers=headers)
data = response.json()
posts = data["data"]["children"]

for post in posts:
    message = json.dumps(post["data"]).encode("utf-8")
    future = publisher.publish(topic_path, message)
    print(f"Sent: {post['data']['title']}")

print("All messages sent!")

while True:
    time.sleep(6000)
