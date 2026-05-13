"""
Database engine factory for the Northwind ETL pipeline.

Wraps SQLAlchemy engine creation so all phases share the same pool
configuration and credential-reading logic.
"""

import logging
from typing import Optional

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.pool import QueuePool

from src.common.config import DatabaseConfig, get_db_config

logger = logging.getLogger(__name__)

_POOL_SIZE = 5
_MAX_OVERFLOW = 10


def get_engine(config: Optional[DatabaseConfig] = None) -> Engine:
    """Create a SQLAlchemy engine connected to the ETL PostgreSQL database.

    Args:
        config: Explicit DatabaseConfig. If *None*, reads from env via
                get_db_config() (which auto-loads .env).

    Returns:
        Engine with QueuePool (pool_size=5, max_overflow=10, pre-ping enabled).
    """
    cfg = config or get_db_config()
    url = (
        f"postgresql+psycopg2://{cfg.user}:{cfg.password}"
        f"@{cfg.host}:{cfg.port}/{cfg.database}"
    )
    return create_engine(
        url,
        poolclass=QueuePool,
        pool_size=_POOL_SIZE,
        max_overflow=_MAX_OVERFLOW,
        pool_pre_ping=True,
    )


def test_connection(config: Optional[DatabaseConfig] = None) -> bool:
    """Verify the PostgreSQL connection is reachable and log the result.

    Args:
        config: Explicit DatabaseConfig. If *None*, reads from env.

    Returns:
        True if ``SELECT 1`` succeeds.

    Raises:
        sqlalchemy.exc.OperationalError: If the database is unreachable.
        RuntimeError: If the query returns an unexpected result.
    """
    engine = get_engine(config)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).scalar()

    if result == 1:
        logger.info(
            "DB connection OK — host=%s port=%s db=%s",
            engine.url.host,
            engine.url.port,
            engine.url.database,
        )
        return True

    raise RuntimeError(f"Unexpected result from SELECT 1: {result!r}")
