from pydantic_settings import BaseSettings

from pydantic import Field, ConfigDict
from typing import Any


class Settings(BaseSettings):
    """
    Shared application settings loaded from environment variables.
    """

    # — Kafka —
    kafka_servers: str = Field(
        "localhost:9092", description="Comma-separated bootstrap servers"
    )
    kafka_api_raw_topic: str = Field(
        "api-raw-ingest", description="Topic for raw ingestion from the API"
    )
    kafka_api_raw_group: str = Field(
        "api-raw-sink", description="Consumer group for raw sink from the API"
    )

    # — Postgres —
    postgres_dsn: str = Field(
        "postgres://meta_user:meta_pass@localhost:5432/metadata",
        description="Full DSN, e.g. postgres://user:pw@host/db",
    )

    db_min_size: int = Field(
        10,
        description="Number of connections the asyncpg Pool is initiated with.",
    )

    db_max_size: int = Field(
        50,
        description="Max number of connections allowed by the aysncpg Pool.",
    )

    # — Filesystem —
    raw_storage_url: str = Field(
        "file:///data/raw",
        description="Storage location where raw data will be written",
    )

    # Pydantic V2 configuration: no env_file so missing vars cause errors
    model_config = ConfigDict(
        case_sensitive=False,  # env var names are case-insensitive
        env_names={  # explicit mapping to environment variables
            "kafka_servers": "KAFKA_SERVERS",
            "kafka_api_raw_topic": "KAFKA_API_RAW_TOPIC",
            "kafka_api_raw_group": "KAFKA_API_RAW_GROUP",
            "postgres_dsn": "POSTGRES_DSN",
            "raw_storage_url": "RAW_STORAGE_URL",
        },
    )


def __getattr__(name: str) -> Any:
    if name == "settings":
        return Settings()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
