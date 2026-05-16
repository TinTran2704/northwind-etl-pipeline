"""
DimensionManager — Kimball Subsystem #17.

Publishes conformed dimension DataFrames to versioned Parquet files and
provides read access to the latest or a specific version.

Layout on disk:
  {base_dir}/{dim_name}/latest.parquet      ← always the most-recent version
  {base_dir}/{dim_name}/{batch_id}.parquet  ← versioned snapshot
"""

import logging
import shutil
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

_DEFAULT_BASE_DIR = Path("data/staging/conform/published")


class DimensionManagerError(Exception):
    """Raised when a dimension cannot be published or read."""


class DimensionManager:
    """Publish and retrieve conformed dimension DataFrames.

    Args:
        base_dir: Root directory for published dimensions.
    """

    def __init__(self, base_dir: Path = _DEFAULT_BASE_DIR) -> None:
        self._base_dir = base_dir

    def publish(self, dim_name: str, df: pd.DataFrame, batch_id: str) -> str:
        """Persist *df* as ``latest.parquet`` and a versioned snapshot.

        Validates that the DataFrame is non-empty before writing.

        Args:
            dim_name: Dimension name (e.g. ``"dim_customer"``).
            df:       Golden records to publish.
            batch_id: ETL batch identifier used as the version tag.

        Returns:
            *batch_id* (the published version string).

        Raises:
            DimensionManagerError: If *df* is empty.
        """
        if df.empty:
            raise DimensionManagerError(f"publish: empty DataFrame for {dim_name!r} — nothing to publish")

        dim_dir = self._base_dir / dim_name
        dim_dir.mkdir(parents=True, exist_ok=True)

        versioned_path = dim_dir / f"{batch_id}.parquet"
        latest_path = dim_dir / "latest.parquet"

        tmp_path = versioned_path.with_suffix(".tmp")
        try:
            df.to_parquet(tmp_path, index=False)
            tmp_path.rename(versioned_path)
        except Exception as exc:
            tmp_path.unlink(missing_ok=True)
            raise DimensionManagerError(f"publish: failed to write {versioned_path}: {exc}") from exc

        shutil.copy2(versioned_path, latest_path)

        logger.info(
            "[CONFORM] published dim=%s version=%s rows=%d → %s",
            dim_name, batch_id, len(df), versioned_path,
        )
        return batch_id

    def get_latest(self, dim_name: str) -> pd.DataFrame:
        """Return the most-recently published version of *dim_name*.

        Args:
            dim_name: Dimension name.

        Returns:
            DataFrame from ``latest.parquet``.

        Raises:
            DimensionManagerError: If no published version exists.
        """
        path = self._base_dir / dim_name / "latest.parquet"
        if not path.exists():
            raise DimensionManagerError(f"get_latest: no published version for {dim_name!r} at {path}")
        return pd.read_parquet(path)

    def get_version(self, dim_name: str, batch_id: str) -> pd.DataFrame:
        """Return the specific *batch_id* version of *dim_name*.

        Args:
            dim_name: Dimension name.
            batch_id: Version identifier.

        Returns:
            DataFrame from ``{batch_id}.parquet``.

        Raises:
            DimensionManagerError: If the requested version does not exist.
        """
        path = self._base_dir / dim_name / f"{batch_id}.parquet"
        if not path.exists():
            raise DimensionManagerError(
                f"get_version: version {batch_id!r} of {dim_name!r} not found at {path}"
            )
        return pd.read_parquet(path)

    def list_versions(self, dim_name: str) -> list[str]:
        """List all available batch_id versions for *dim_name*.

        Returns:
            Sorted list of batch_id strings (newest last by string sort).
        """
        dim_dir = self._base_dir / dim_name
        if not dim_dir.exists():
            return []
        return sorted(
            p.stem for p in dim_dir.glob("*.parquet") if p.stem != "latest"
        )
