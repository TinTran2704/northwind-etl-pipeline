"""
Configuration helpers for the Northwind ETL pipeline.

Reads credentials from .env (via python-dotenv) and source definitions
from config/sources.yaml. Never hard-codes values.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import yaml
from dotenv import load_dotenv

_DEFAULT_SOURCES_PATH = Path("config/sources.yaml")


@dataclass(frozen=True)
class DatabaseConfig:
    """PostgreSQL connection parameters.

    Attributes:
        host: Database host address.
        port: Database port (default 5432).
        user: Login user.
        password: Login password.
        database: Target database name.
    """

    host: str
    port: int
    user: str
    password: str
    database: str


def load_env(env_path: Optional[Path] = None) -> None:
    """Load a .env file into os.environ using python-dotenv.

    Existing environment variables are NOT overridden (override=False),
    so variables already set by the OS or a container take precedence.

    Args:
        env_path: Path to .env file. Defaults to ``.env`` in cwd.
    """
    load_dotenv(dotenv_path=env_path or Path(".env"), override=False)


def load_sources_config(path: Path = _DEFAULT_SOURCES_PATH) -> dict[str, Any]:
    """Load and return the sources configuration from YAML.

    Args:
        path: Path to sources.yaml.

    Returns:
        Parsed YAML content as a plain dict.

    Raises:
        FileNotFoundError: If the file does not exist at *path*.
    """
    if not path.exists():
        raise FileNotFoundError(f"Sources config not found: {path}")
    with path.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def get_db_config() -> DatabaseConfig:
    """Build a DatabaseConfig from ETL_DW_* environment variables.

    Calls load_env() first so the function is self-contained when invoked
    from a plain Python script (no external dotenv loader needed).

    Expected env vars::

        ETL_DW_HOST      (default: localhost)
        ETL_DW_PORT      (default: value of POSTGRES_PORT, then 5432)
        ETL_DW_USER      (required)
        ETL_DW_PASSWORD  (required)
        ETL_DW_DATABASE  (required)

    Returns:
        Populated DatabaseConfig.

    Raises:
        EnvironmentError: If any required variable is absent.
    """
    load_env()

    required = {"ETL_DW_USER", "ETL_DW_PASSWORD", "ETL_DW_DATABASE"}
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variable(s): {', '.join(sorted(missing))}"
        )

    port_str = (
        os.environ.get("ETL_DW_PORT")
        or os.environ.get("POSTGRES_PORT")
        or "5432"
    )

    return DatabaseConfig(
        host=os.environ.get("ETL_DW_HOST", "localhost"),
        port=int(port_str),
        user=os.environ["ETL_DW_USER"],
        password=os.environ["ETL_DW_PASSWORD"],
        database=os.environ["ETL_DW_DATABASE"],
    )
