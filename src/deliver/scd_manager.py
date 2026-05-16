"""
SCD Manager — Kimball Subsystem #9.

Implements Slowly Changing Dimension logic for Type 1 and Type 2 attributes.

apply_scd() returns:
  (updated_dim, change_log)

change_log columns: nk | action | old_sk | new_sk
Actions:
  INSERT            — new NK not in existing dim
  UPDATE_T1         — type-1 attribute changed, all versions updated in-place
  UPDATE_T2_EXPIRE  — old current row expired (type-2 change detected)
  UPDATE_T2_NEW     — new row inserted after type-2 expiry
  EXPIRE            — NK vanished from source; old row closed out
"""

import logging
from datetime import date, timedelta
from typing import Any, Optional

import pandas as pd

from src.deliver.surrogate_key_generator import SurrogateKeyGenerator

logger = logging.getLogger(__name__)


class SCDError(Exception):
    """Raised on unrecoverable SCD processing error."""


class SCDManager:
    """Apply SCD Type 1 / Type 2 logic to a dimension DataFrame.

    Args:
        sk_gen: SurrogateKeyGenerator for new SK allocation.
    """

    def __init__(self, sk_gen: SurrogateKeyGenerator) -> None:
        self._sk_gen = sk_gen

    def apply_scd(
        self,
        new_rows: pd.DataFrame,
        existing_dim: pd.DataFrame,
        nk_col: str,
        sk_col: str,
        type2_cols: list[str],
        type1_cols: list[str],
        effective_date: date,
        dim_name: str = "dim_unknown",
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Apply SCD logic and return (updated_dim, change_log).

        For an **initial load** pass an empty *existing_dim* (or one with only
        the Unknown member row).  Every NK in *new_rows* becomes an INSERT.

        Args:
            new_rows:       Conformed golden records (one per NK).
            existing_dim:   Current state of the dimension table.
            nk_col:         Natural key column name (present in both DataFrames).
            sk_col:         Surrogate key column name in *existing_dim*.
            type2_cols:     Columns whose changes trigger a new SCD-2 row.
            type1_cols:     Columns whose changes update all existing rows in-place.
            effective_date: Date to use for new rows and expiry dates.
            dim_name:       Used for SK generation (e.g. ``"dim_customer"``).

        Returns:
            Tuple of (updated full dim DataFrame, change_log DataFrame).
        """
        expire_date = effective_date - timedelta(days=1)

        # Separate Unknown member from working set
        if not existing_dim.empty and sk_col in existing_dim.columns:
            unknown_mask = existing_dim[sk_col] == -1
            unknown_rows = existing_dim[unknown_mask].copy()
            working_existing = existing_dim[~unknown_mask].copy()
        else:
            unknown_rows = pd.DataFrame(columns=existing_dim.columns if not existing_dim.empty else [])
            working_existing = existing_dim.copy() if not existing_dim.empty else pd.DataFrame()

        # Index current rows by NK
        if not working_existing.empty and "is_current" in working_existing.columns:
            current_rows = working_existing[working_existing["is_current"] == True].copy()
        else:
            current_rows = working_existing.copy()

        current_by_nk: dict[Any, pd.Series] = {
            row[nk_col]: row for _, row in current_rows.iterrows()
        }

        change_log_rows: list[dict] = []
        new_dim_rows: list[dict] = []
        updates_t1: list[dict] = []  # {nk: ..., col: new_value, ...}

        new_nks = set(new_rows[nk_col].dropna())

        for _, new_row in new_rows.iterrows():
            nk = new_row[nk_col]
            current = current_by_nk.get(nk)

            if current is None:
                # NK not in existing dim → INSERT
                new_sk = self._sk_gen.next_sk(dim_name)
                row_dict = self._build_new_row(
                    new_row, new_sk, effective_date, nk_col, sk_col,
                    type2_cols, type1_cols,
                )
                new_dim_rows.append(row_dict)
                change_log_rows.append({"nk": nk, "action": "INSERT",
                                         "old_sk": None, "new_sk": new_sk})
                logger.debug("SCD INSERT nk=%s sk=%d", nk, new_sk)

            else:
                old_sk = int(current[sk_col]) if sk_col in current.index else -1
                diff_t2 = self._has_changed(new_row, current, type2_cols)
                diff_t1 = self._has_changed(new_row, current, type1_cols)

                if diff_t2:
                    # Expire old current row
                    change_log_rows.append({"nk": nk, "action": "UPDATE_T2_EXPIRE",
                                             "old_sk": old_sk, "new_sk": None})
                    # Insert new current row
                    new_sk = self._sk_gen.next_sk(dim_name)
                    row_dict = self._build_new_row(
                        new_row, new_sk, effective_date, nk_col, sk_col,
                        type2_cols, type1_cols,
                    )
                    new_dim_rows.append(row_dict)
                    change_log_rows.append({"nk": nk, "action": "UPDATE_T2_NEW",
                                             "old_sk": old_sk, "new_sk": new_sk})
                    logger.debug("SCD TYPE2 nk=%s old_sk=%d new_sk=%d", nk, old_sk, new_sk)

                    # Apply type-1 diffs on new row (already in row_dict)
                    if diff_t1:
                        updates_t1.append({nk_col: nk, **{c: new_row.get(c) for c in type1_cols}})

                elif diff_t1:
                    updates_t1.append({nk_col: nk, **{c: new_row.get(c) for c in type1_cols}})
                    change_log_rows.append({"nk": nk, "action": "UPDATE_T1",
                                             "old_sk": old_sk, "new_sk": old_sk})
                    logger.debug("SCD TYPE1 nk=%s sk=%d", nk, old_sk)

        # Build updated dim: start from working_existing
        if working_existing.empty:
            updated_dim = pd.DataFrame(new_dim_rows) if new_dim_rows else pd.DataFrame()
        else:
            updated_dim = working_existing.copy()

            # Apply type-2 expiry on old current rows that have a T2 change
            t2_expire_nks = {
                r["nk"] for r in change_log_rows if r["action"] == "UPDATE_T2_EXPIRE"
            }
            if t2_expire_nks and "is_current" in updated_dim.columns:
                mask = (updated_dim[nk_col].isin(t2_expire_nks)) & (updated_dim["is_current"] == True)
                updated_dim.loc[mask, "expiration_date"] = str(expire_date)
                updated_dim.loc[mask, "is_current"] = False

            # Apply type-1 updates across all rows for affected NKs
            for upd in updates_t1:
                nk_val = upd[nk_col]
                for col, val in upd.items():
                    if col == nk_col:
                        continue
                    if col in updated_dim.columns:
                        updated_dim.loc[updated_dim[nk_col] == nk_val, col] = val

            # Expire NKs that vanished from source
            current_nks_in_dim = set(current_by_nk.keys())
            vanished = current_nks_in_dim - new_nks
            if vanished and "is_current" in updated_dim.columns:
                mask = (updated_dim[nk_col].isin(vanished)) & (updated_dim["is_current"] == True)
                updated_dim.loc[mask, "expiration_date"] = str(expire_date)
                updated_dim.loc[mask, "is_current"] = False
                for nk in vanished:
                    old_sk = int(current_by_nk[nk][sk_col]) if sk_col in current_by_nk[nk].index else None
                    change_log_rows.append({"nk": nk, "action": "EXPIRE",
                                             "old_sk": old_sk, "new_sk": None})

            # Append new rows
            if new_dim_rows:
                new_df = pd.DataFrame(new_dim_rows)
                updated_dim = pd.concat([updated_dim, new_df], ignore_index=True)

        # Re-attach Unknown member
        if not unknown_rows.empty:
            updated_dim = pd.concat([unknown_rows, updated_dim], ignore_index=True)

        change_log = pd.DataFrame(change_log_rows, columns=["nk", "action", "old_sk", "new_sk"])
        return updated_dim, change_log

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _has_changed(new_row: pd.Series, current: pd.Series, cols: list[str]) -> bool:
        for col in cols:
            new_val = new_row.get(col)
            cur_val = current.get(col)
            # Treat NaN/None as equal
            new_null = new_val is None or (isinstance(new_val, float) and pd.isna(new_val))
            cur_null = cur_val is None or (isinstance(cur_val, float) and pd.isna(cur_val))
            if new_null and cur_null:
                continue
            if new_null != cur_null:
                return True
            if str(new_val).strip() != str(cur_val).strip():
                return True
        return False

    @staticmethod
    def _build_new_row(
        source: pd.Series,
        sk: int,
        eff: date,
        nk_col: str,
        sk_col: str,
        type2_cols: list[str],
        type1_cols: list[str],
    ) -> dict:
        row = source.to_dict()
        row[sk_col] = sk
        row["effective_date"] = str(eff)
        row["expiration_date"] = None
        row["is_current"] = True
        return row
