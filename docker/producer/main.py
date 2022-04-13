#!/usr/bin/python3

import os
import time
import json
from tweepy import OAuthHandler
from tweepy import Stream

from kafka import KafkaProducer
import kafka.errors

from decouple import config

KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC = "twitter"

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER.split(","))
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)


class ListenerTwitter(Stream):
    def on_status(self, status):
        if hasattr(status, 'extended_tweet'):
            text = status.extended_tweet('full_text')
        else:
            text = status.text

        user = status.user.screen_name
        value = {'text': text, 'user': user}
        value_json = json.dumps(value)

        print(value_json)

        return True

listener = ListenerTwitter(API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_SECRET)


