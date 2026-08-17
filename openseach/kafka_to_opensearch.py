import json
import logging
import os
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer
from opensearchpy import OpenSearch, helpers

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("KafkaToOpenSearch")

KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
OS_HOST = os.getenv("OPENSEARCH_HOST")
OS_PORT = int(os.getenv("OPENSEARCH_PORT"))
OS_USER = os.getenv("OPENSEARCH_USER")
OS_PASS = os.getenv("OPENSEARCH_PASSWORD")

TOPIC_INDEX_PREFIX = {
    "stock_raw":       "financial-stocks",
    "crypto_raw":      "financial-crypto",
    "forex_raw":       "financial-forex",
    "ml_predictions":  "financial-ml-predictions",
}


def _daily_index(prefix: str) -> str:
    return f"{prefix}-{datetime.now(timezone.utc).strftime('%Y.%m.%d')}"


def _wait_for_opensearch(client: OpenSearch, retries: int = 30):
    for i in range(retries):
        try:
            if client.ping():
                log.info("OpenSearch is reachable")
                return
        except Exception:
            pass
        log.info(f"Waiting for OpenSearch ({i+1}/{retries}) …")
        time.sleep(5)
    raise RuntimeError("OpenSearch is not reachable")


def _wait_for_kafka(retries: int = 30) -> KafkaConsumer:
    for i in range(retries):
        try:
            consumer = KafkaConsumer(
                *TOPIC_INDEX_PREFIX.keys(),
                bootstrap_servers=KAFKA_BROKERS,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                group_id="opensearch-bridge",
                consumer_timeout_ms=1000,
                enable_auto_commit=True,
            )
            log.info("Connected to Kafka")
            return consumer
        except Exception as e:
            log.info(f"Waiting for Kafka ({i+1}/{retries}): {e}")
            time.sleep(5)
    raise RuntimeError("Kafka is not reachable")


def _enrich(topic: str, data: dict) -> dict:
    ts = data.get("timestamp") or data.get("event_time") or datetime.now(timezone.utc).isoformat()
    data["@timestamp"] = ts
    data.pop("raw_payload", None)  # strip large nested payload
    return data


def run():
    os_client = OpenSearch(
        hosts=[{"host": OS_HOST, "port": OS_PORT}],
        http_auth=(OS_USER, OS_PASS),
        use_ssl=False,
        verify_certs=False,
    )
    _wait_for_opensearch(os_client)

    consumer = _wait_for_kafka()

    log.info("Bridge running — consuming from %s", list(TOPIC_INDEX_PREFIX.keys()))

    buffer: list[dict] = []
    FLUSH_SIZE = 50

    while True:
        try:
            for msg in consumer:
                topic = msg.topic
                data = _enrich(topic, msg.value)
                prefix = TOPIC_INDEX_PREFIX[topic]
                buffer.append({
                    "_index": _daily_index(prefix),
                    "_source": data,
                })
                if len(buffer) >= FLUSH_SIZE:
                    _flush(os_client, buffer)
                    buffer.clear()
        except StopIteration:
            # consumer_timeout_ms expired — flush partial batch and keep polling
            if buffer:
                _flush(os_client, buffer)
                buffer.clear()
        except Exception as e:
            log.error("Unexpected error: %s", e)
            time.sleep(2)


def _flush(client: OpenSearch, docs: list[dict]):
    try:
        success, errors = helpers.bulk(client, docs, raise_on_error=False)
        if errors:
            log.warning("Bulk index had %d errors", len(errors))
        else:
            log.info("Indexed %d documents", success)
    except Exception as e:
        log.error("Bulk index failed: %s", e)


if __name__ == "__main__":
    run()
