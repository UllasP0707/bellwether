"""Runtime configuration, read from the environment once at import time."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Topics:
    """Topic names. Centralized so a rename is one edit."""

    RAW = "bellwether.events.raw"
    NORMALIZED = "bellwether.events.normalized"
    SCORES = "bellwether.risk.scores"
    INTERVENTIONS = "bellwether.interventions"
    # Anything a stage understood well enough to route but not well enough to
    # trust. Kept out of the main path so one poisoned message cannot stall a
    # partition, and kept rather than dropped so it can be inspected.
    DLQ = "bellwether.events.dlq"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="BELLWETHER_",
        env_file=".env",
        extra="ignore",
    )

    tenant_id: str = "acme"

    kafka_bootstrap: str = "localhost:9092"
    schema_registry: str = "http://localhost:8081"

    postgres_dsn: str = "postgresql://bellwether:bellwether@localhost:5432/bellwether"
    redis_url: str = "redis://localhost:6379/0"

    s3_endpoint: str = "http://localhost:9000"
    s3_bucket: str = "bellwether-lake"
    s3_access_key: str = "bellwether"
    s3_secret_key: str = "bellwetherbellwether"

    log_level: str = "INFO"

    # Scoring lookback. Events older than this stop contributing, which bounds
    # both the streaming window and the batch scan.
    score_lookback_days: int = Field(default=30, ge=1, le=365)


@lru_cache
def settings() -> Settings:
    return Settings()
