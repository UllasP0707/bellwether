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

    # API credentials, as `key:tenant:actor` triples separated by commas. The
    # tenant a request may read is a property of the key and never of the
    # request, so there is no parameter a caller can set to change it.
    api_keys: str = "localdev:acme:analyst"

    # Scoring lookback. Events older than this stop contributing, which bounds
    # both the streaming window and the batch scan.
    score_lookback_days: int = Field(default=30, ge=1, le=365)

    # Intervention copy. The writer is a plug: `auto` picks whichever
    # credential is present and falls back to static templates when neither is,
    # which is a supported way to run this rather than a degraded one.
    #
    # `chat` is any OpenAI-compatible /chat/completions endpoint — OpenRouter,
    # vLLM, Ollama, a gateway. Naming the protocol rather than the vendor is
    # the point: which model writes the copy is a deployment decision, and the
    # guardrails downstream do not care and must not.
    copy_provider: str = "auto"  # auto | chat | anthropic | template
    copy_base_url: str = "https://openrouter.ai/api/v1"
    copy_model: str = ""
    copy_api_key: str = ""
    # Generous next to the rest of the pipeline, because a reasoning model
    # spends most of its time before emitting a token. Affordable only because
    # drafts are cached by brief shape: a timeout this long in front of a call
    # made once per message would put the whole partition behind one email.
    copy_timeout_seconds: float = Field(default=45.0, gt=0)

    # Observability. Both are opt-in: with no endpoint set, tracing installs
    # nothing, and with no port set, no stage opens a socket. Monitoring that
    # can stop a stage from starting is worse than no monitoring.
    otlp_endpoint: str = ""
    metrics_port: int = 0

    # Field-level tokenization. One secret, one derived key per tenant, so
    # adding a tenant needs no new configuration and no tenant's tokens can be
    # computed from another's. Destroying a key is the only erasure that
    # reaches a Parquet file or a Kafka segment.
    tokenization_secret: str = ""


@lru_cache
def settings() -> Settings:
    return Settings()
