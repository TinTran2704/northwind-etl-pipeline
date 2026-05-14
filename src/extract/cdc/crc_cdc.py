"""
CRC-based Change Data Capture. Implements Kimball Subsystem #2.

Detects inserts, updates, and deletes between two full snapshots by
computing CRC32 fingerprints for each row and comparing on primary key.
See docs/05-extract-phase.md §5.4.
"""

import logging
import zlib
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

_CRC_COLUMN = "_crc"


class CrcCdc:
    """CRC32-based Change Data Capture between full snapshots.

    Args:
        entity:         Entity name used for the CRC index filename,
                        e.g. ``"customers"``.
        pk_columns:     List of primary-key column names used to match rows
                        between old and new snapshots.
        crc_index_dir:  Directory where CRC index parquet files are stored.
                        Defaults to ``data/staging/_crc_index``.
    """

    def __init__(
        self,
        entity: str,
        pk_columns: list[str],
        crc_index_dir: Path = Path("data/staging/_crc_index"),
    ) -> None:
        self.entity = entity
        self.pk_columns = pk_columns
        self.crc_index_dir = crc_index_dir

    # ── Public API ────────────────────────────────────────────────────────────

    def compute_crc(self, df: pd.DataFrame) -> pd.Series:
        """Compute CRC32 fingerprint for each row in *df*.

        All column values are serialised as strings and joined with ``|``
        before hashing. Returns an unsigned 32-bit integer per row.

        Args:
            df: Input DataFrame (columns must be in a stable order).

        Returns:
            pd.Series of uint32 CRC values, same index as *df*.
        """
        def _row_crc(row: pd.Series) -> int:
            content = "|".join(str(v) for v in row)
            return zlib.crc32(content.encode("utf-8")) & 0xFFFFFFFF

        return df.apply(_row_crc, axis=1)

    def diff(
        self, old_df: pd.DataFrame, new_df: pd.DataFrame
    ) -> dict[str, pd.DataFrame]:
        """Compare two full snapshots and return row-level changes.

        On the first run pass an empty DataFrame as *old_df*; every row in
        *new_df* will be classified as an insert.

        Args:
            old_df: Previous snapshot (all columns, no CRC column).
            new_df: Current snapshot (all columns, no CRC column).

        Returns:
            Dict with keys ``"inserts"``, ``"updates"``, ``"deletes"``, each
            containing a DataFrame of the relevant rows from *new_df*
            (inserts/updates) or *old_df* (deletes).
        """
        if old_df.empty:
            logger.info(
                "[CDC] entity=%s first run — all %d rows are inserts",
                self.entity, len(new_df),
            )
            return {
                "inserts": new_df.copy(),
                "updates": pd.DataFrame(columns=new_df.columns),
                "deletes": pd.DataFrame(columns=new_df.columns),
            }

        old_crcs = old_df[self.pk_columns].copy()
        old_crcs[_CRC_COLUMN] = self.compute_crc(old_df)

        new_crcs = new_df[self.pk_columns].copy()
        new_crcs[_CRC_COLUMN] = self.compute_crc(new_df)

        merged = pd.merge(
            old_crcs,
            new_crcs,
            on=self.pk_columns,
            how="outer",
            suffixes=("_old", "_new"),
            indicator=True,
        )

        inserts = self._rows_for_pks(
            new_df,
            merged[merged["_merge"] == "right_only"][self.pk_columns],
        )
        deletes = self._rows_for_pks(
            old_df,
            merged[merged["_merge"] == "left_only"][self.pk_columns],
        )
        both = merged[merged["_merge"] == "both"]
        update_pks = both[
            both[f"{_CRC_COLUMN}_old"] != both[f"{_CRC_COLUMN}_new"]
        ][self.pk_columns]
        updates = self._rows_for_pks(new_df, update_pks)

        logger.info(
            "[CDC] entity=%s inserts=%d updates=%d deletes=%d",
            self.entity, len(inserts), len(updates), len(deletes),
        )
        return {"inserts": inserts, "updates": updates, "deletes": deletes}

    def save_crc_index(self, df: pd.DataFrame) -> Path:
        """Persist the current snapshot CRC index to parquet.

        Stores all columns of *df* plus a ``_crc`` column so that deleted
        rows can be fully reconstructed on the next run.

        Args:
            df: Current full snapshot DataFrame.

        Returns:
            Path to the written parquet file.
        """
        self.crc_index_dir.mkdir(parents=True, exist_ok=True)
        crc_path = self.crc_index_dir / f"{self.entity}.parquet"
        index_df = df.copy()
        index_df[_CRC_COLUMN] = self.compute_crc(df)
        index_df.to_parquet(crc_path, index=False)
        logger.info("[CDC] CRC index saved → %s (%d rows)", crc_path, len(df))
        return crc_path

    def load_crc_index(self) -> Optional[pd.DataFrame]:
        """Load previous snapshot from the CRC index parquet.

        Returns:
            DataFrame with all source columns plus ``_crc``, or ``None`` if
            no index exists (first run).
        """
        crc_path = self.crc_index_dir / f"{self.entity}.parquet"
        if not crc_path.exists():
            logger.info("[CDC] entity=%s no prior CRC index — first run", self.entity)
            return None
        df = pd.read_parquet(crc_path)
        logger.info("[CDC] CRC index loaded ← %s (%d rows)", crc_path, len(df))
        return df

    # ── Private helpers ───────────────────────────────────────────────────────

    def _rows_for_pks(
        self, source_df: pd.DataFrame, pk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Inner-join *source_df* with the given PK subset."""
        if pk_df.empty:
            return pd.DataFrame(columns=source_df.columns)
        return pd.merge(source_df, pk_df, on=self.pk_columns, how="inner").reset_index(
            drop=True
        )
