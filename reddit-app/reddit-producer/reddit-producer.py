import os
import requests
from google.cloud import pubsub_v1
from google.oauth2 import service_account
import json
import time

PROJECT_ID = "norse-geode-477321-v2"
TOPIC_ID = "reddit-topic"

# 🔧 FIX: use raw string or forward slashes
credentials = r"private-key.json"
creds = service_account.Credentials.from_service_account_file(credentials)
publisher = pubsub_v1.PublisherClient(credentials=creds)
print(publisher)

topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

url = "https://www.reddit.com/r/dataengineering/top.json?limit=10&t=all"
headers = {"User-Agent": "dataengineering-lab-script/1.0"}

print("Fetching posts from Reddit API...")
response = requests.get(url, headers=headers)
data = response.json()
posts = data["data"]["children"]

for post in posts:
    message = json.dumps(post["data"]).encode("utf-8")
    #future = publisher.publish(TOPIC_ID, message)#topic_path, message)
    future = publisher.publish(topic_path, message)
    print(f"Sent: {post['data']['title']}")

print("All messages sent!")

while True:
    time.sleep(6000)
