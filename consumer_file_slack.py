from kafka import KafkaConsumer
from slack_sdk import WebClient
import requests
import json

# Kafka broker details
bootstrap_servers = "localhost:9092"
topic = "cdc.public.transactions"

SLACK_TOKEN = "****"
client = WebClient(token=SLACK_TOKEN)

# Create Kafka consumer
consumer = KafkaConsumer(
    topic,
    group_id="my_consumer_group",
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: x.decode("utf-8") if x else None,
)

def send_response_message(message):
    try:
        message_dict = json.loads(message)
        payload = message_dict.get("payload")

        before = payload.get("before")
        after = payload.get("after")

        if before and after:
            transaction_id = before.get("transaction_id")
            order_status_before = before.get("order_status")
            order_status_after = after.get("order_status")
            change_info = after.get("change_info")

            if order_status_after == "CANCELLED" and order_status_before != "CANCELLED":
                text = f"Transaction cancelled tid: {transaction_id}, change_info: {change_info}"
                send_slack_message(text)
                return text
    except json.JSONDecodeError:
        print("Failed to decode JSON from message")
    except KeyError as e:
        print(f"Missing expected key: {e}")
    return None

# Function to send message to Slack
def send_slack_message(msg):
    try:
        response = client.chat_postMessage(
            channel="data-engineering-project-1", text=msg, username="Bot User"
        )
        if not response["ok"]:
            print(f"Failed to send message to Slack: {response['error']}")
    except Exception as e:
        print(f"Error sending message to Slack: {e}")

# Consume messages from Kafka and send to Slack
for message in consumer:
    message_value = message.value

    if message_value:
        return_value = send_response_message(message_value)
        if return_value:
            print("Received message: ", return_value)
    else:
        print("Received empty message")
