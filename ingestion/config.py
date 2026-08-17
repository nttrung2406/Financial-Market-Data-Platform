from pydantic_settings import BaseSettings


class Settings(BaseSettings):

    # External APIs
    twelve_data_endpoint: str
    twelve_data_api_key: str
    exchange_rate_endpoint: str
    exchange_rate_api_key: str
    binance_endpoint: str
    binance_api_key: str
    binance_api_secret_key: str
    binance_hmac: str

    # Kafka
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic_stocks: str = "stock_raw"
    kafka_topic_crypto: str = "crypto_raw"
    kafka_topic_forex: str = "forex_raw"

    # MinIO — field names match .env keys (MINIO_ROOT_USER / MINIO_ROOT_PASSWORD)
    minio_root_user: str = "minioadmin"
    minio_root_password: str = "minioadmin"
    minio_endpoint: str = "minio:9000"
    minio_bucket: str = "raw"
    minio_secure: bool = False

    # PostgreSQL Warehouse
    postgres_db: str = "finance"
    postgres_user: str = "finance"
    postgres_password: str = "finance"
    postgres_host: str = "postgres"
    postgres_port: int = 5432

    @property
    def minio_access_key(self) -> str:
        return self.minio_root_user

    @property
    def minio_secret_key(self) -> str:
        return self.minio_root_password

    @property
    def postgres_jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()

