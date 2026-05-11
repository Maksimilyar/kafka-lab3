import csv
import json
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

TOPICS = ["Topic1", "Topic2"]

# Очікування Kafka
while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers='broker1:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Connected to Kafka!")
        break

    except NoBrokersAvailable:
        print("Kafka is not ready yet... retrying in 5 seconds")
        time.sleep(5)

# Читання CSV
with open('Divvy_Trips_2019_Q4.csv', 'r', encoding='utf-8') as file:
    reader = csv.DictReader(file)

    for row in reader:
        print(f"Sending: {row}")

        for topic in TOPICS:
            producer.send(topic, row)

        producer.flush()

        time.sleep(1)

print("All messages sent!")