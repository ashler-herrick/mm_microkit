import os
import subprocess
from pathlib import Path
import time

import pytest


COMPOSE_FILE = Path(__file__).parent.parent / "docker-compose.yml"
ENV = os.environ.copy()
RAW_STORAGE = Path(__file__).parent / "test_data"
ENV.update(
    {
        "POSTGRES_DSN": "postgres://meta_user:meta_pass@localhost:5432/metadata",
        "KAFKA_SERVERS": "localhost:9092",
        "KAFKA_RAW_TOPIC": "test-topic",
        "KAFKA_GROUP": "test-group",
        "RAW_STORAGE_URL": "",
    }
)

@pytest.fixture(scope="session", autouse=True)
def docker_compose_up():
    """
    Bring up docker-compose stack for the entire test session.
    """
    cmd = ["docker", "compose", "-f", str(COMPOSE_FILE), "up", "-d"]
    subprocess.run(cmd, check=True, env=ENV)

    # wait for Postgres
    time.sleep(3)
    # wait for Kafka (broker + topic auto-creation)
    time.sleep(3)

    yield

    # tear down
    subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "down", "--volumes"],
        check=True,
        env=ENV,
    )


if __name__ == "__main__":
    print(COMPOSE_FILE)
    print(RAW_STORAGE)
