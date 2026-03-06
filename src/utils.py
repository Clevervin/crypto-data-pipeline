from __future__ import annotations

import logging
from pathlib import Path

import yaml
from sqlalchemy import create_engine


def load_config(config_path: str = "config/config.yaml") -> dict:
    path = Path(config_path)

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    return config


def get_db_engine(config: dict):
    db = config["postgres"]

    connection_string = (
        f"postgresql+psycopg2://{db['user']}:{db['password']}@"
        f"{db['host']}:{db['port']}/{db['dbname']}"
    )

    engine = create_engine(connection_string)

    return engine


def setup_logger(name: str, level: str = "INFO"):

    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / "pipeline.log"

    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    return logging.getLogger(name)