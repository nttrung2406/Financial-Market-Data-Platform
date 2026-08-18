import io
import json
from typing import Dict, Any
from src.ingestion.base.logger import get_logger

logger = get_logger("MinIOClient")


class MinIOStorageService:
    """MinIO Client Wrapper with bucket initialization and JSON uploads"""

    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket_name: str, secure: bool = False):
        self.endpoint = endpoint
        self.bucket_name = bucket_name
        self.client = None
        self._init_client(access_key, secret_key, secure)

    def _init_client(self, access_key: str, secret_key: str, secure: bool):
        try:
            from minio import Minio
            self.client = Minio(
                endpoint=self.endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure,
            )
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
                logger.info(f"MinIO Created Bucket: '{self.bucket_name}'")
            else:
                logger.info(f"MinIO Connected to Bucket: '{self.bucket_name}'")
        except Exception as e:
            logger.warning(f"Could not connect to MinIO ({e}). Running in fallback mode.")
            self.client = None

    def upload_json(self, object_name: str, data: Dict[str, Any]) -> bool:
        json_bytes = json.dumps(data, default=str).encode("utf-8")
        stream = io.BytesIO(json_bytes)

        if self.client:
            try:
                self.client.put_object(
                    bucket_name=self.bucket_name,
                    object_name=object_name,
                    data=stream,
                    length=len(json_bytes),
                    content_type="application/json",
                )
                logger.info(f"[MinIO] Saved s3://{self.bucket_name}/{object_name}")
                return True
            except Exception as e:
                logger.error(f"[MinIO] Failed upload ({object_name}): {e}")
                return False
        else:
            logger.info(f"[Dry-Run MinIO] Upload s3://{self.bucket_name}/{object_name}")
            return True