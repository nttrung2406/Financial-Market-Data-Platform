import json
from typing import Dict, Any, Optional
from src.ingestion.base.logger import get_logger

logger = get_logger("KafkaProducer")


class KafkaProducerService:
    """Kafka Producer Wrapper with fallback for offline mode"""

    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self._init_producer()

    def _init_producer(self):
        try:
            from kafka import KafkaProducer
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                retries=3,
            )
            logger.info(f"Connected to Kafka broker at {self.bootstrap_servers}")
        except Exception as e:
            logger.warning(f"Could not connect to Kafka broker ({e}). Running in fallback mode.")
            self.producer = None

    def send(self, topic: str, value: Dict[str, Any], key: Optional[str] = None):
        if self.producer:
            try:
                future = self.producer.send(topic=topic, value=value, key=key)
                self.producer.flush()
                logger.info(f"[Kafka] Published message to '{topic}' [key={key}]")
                return future
            except Exception as e:
                logger.error(f"[Kafka] Failed to send message to '{topic}': {e}")
        else:
            logger.info(f"[Dry-Run Kafka] Topic: '{topic}' | Key: '{key}' | Message: {value}")

    def close(self):
        if self.producer:
            self.producer.close()
            logger.info("Kafka Producer closed.")