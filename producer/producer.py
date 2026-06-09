import csv
import json
import os
import time
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
CSV_FILE = os.environ.get("CSV_FILE", "Divvy_Trips_2019_Q4.csv")
TOPIC1 = os.environ.get("TOPIC1", "Topic1")
TOPIC2 = os.environ.get("TOPIC2", "Topic2")
MESSAGE_DELAY = float(os.environ.get("MESSAGE_DELAY", "0.1"))


def wait_for_kafka(servers, retries=15, delay=5):
    """Retry connecting to Kafka until brokers are ready."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(bootstrap_servers=servers, api_version=(2, 5, 0))
            producer.close()
            logger.info("Kafka brokers are ready.")
            return
        except KafkaError as e:
            logger.warning("Attempt %d/%d: Kafka not ready yet (%s). Retrying in %ds...",
                           attempt, retries, e, delay)
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after %d attempts." % retries)


def parse_tripduration(value: str) -> float:
    """Parse tripduration which may contain comma-formatted numbers like '2,350.0'."""
    try:
        return float(value.replace(",", ""))
    except (ValueError, AttributeError):
        return 0.0


def build_message(row: dict) -> dict:
    """Convert a CSV row dict into a typed message dict."""
    return {
        "trip_id": int(row.get("trip_id", 0)),
        "start_time": row.get("start_time", ""),
        "end_time": row.get("end_time", ""),
        "bikeid": int(row.get("bikeid", 0)),
        "tripduration": parse_tripduration(row.get("tripduration", "0")),
        "from_station_id": int(row.get("from_station_id", 0)),
        "from_station_name": row.get("from_station_name", ""),
        "to_station_id": int(row.get("to_station_id", 0)),
        "to_station_name": row.get("to_station_name", ""),
        "usertype": row.get("usertype", ""),
        "gender": row.get("gender", ""),
        "birthyear": row.get("birthyear", ""),
    }


def on_send_success(record_metadata):
    logger.debug(
        "Message sent -> topic=%s partition=%d offset=%d",
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset,
    )


def on_send_error(exc):
    logger.error("Failed to deliver message: %s", exc)


def main():
    wait_for_kafka(BOOTSTRAP_SERVERS)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        acks="all",
        retries=3,
        api_version=(2, 5, 0),
    )

    logger.info("Starting to read CSV: %s", CSV_FILE)
    sent = 0

    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            message = build_message(row)
            key = message["trip_id"]

            # Publish to both topics
            for topic in (TOPIC1, TOPIC2):
                producer.send(topic, key=key, value=message) \
                    .add_callback(on_send_success) \
                    .add_errback(on_send_error)

            sent += 1
            if sent % 1000 == 0:
                producer.flush()
                logger.info("Sent %d messages so far...", sent)

            time.sleep(MESSAGE_DELAY)

    producer.flush()
    logger.info("Done. Total messages sent: %d (each published to %s and %s).",
                sent, TOPIC1, TOPIC2)
    producer.close()


if __name__ == "__main__":
    main()
