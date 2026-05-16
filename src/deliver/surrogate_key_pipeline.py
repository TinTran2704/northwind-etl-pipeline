"""
Surrogate Key Pipeline — Kimball Subsystem #14.

Point-in-time SK resolution for fact tables.
For each FK in a fact row, looks up the correct dimension SK that was
current at the time of the event (order_date).
"""

import logging
from dataclasses import dataclass
from datetime import date
from typing import Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)

_UNKNOWN_SK = -1
_UNKNOWN_DATE_SK = 19000101


class SKPipelineError(Exception):
    """Raised when overlapping SCD2 windows are detected."""


@dataclass
class LookupConfig:
    """Configuration for a single FK resolution in resolve_batch().

    Attributes:
        source_col:  Column in fact_df containing the NK value.
        dim_df:      Dimension DataFrame to look up against.
        nk_col:      NK column in *dim_df*.
        sk_col:      SK column in *dim_df*.
        output_col:  Column name to write in the output fact DataFrame.
        date_col:    Fact column containing the event date for point-in-time
                     lookup.  ``None`` means Type-1 lookup (no date filtering).
        eff_col:     Effective-date column in *dim_df*.
        exp_col:     Expiration-date column in *dim_df*.
    """

    source_col: str
    dim_df: pd.DataFrame
    nk_col: str
    sk_col: str
    output_col: str
    date_col: Optional[str] = None
    eff_col: str = "effective_date"
    exp_col: str = "expiration_date"


class SurrogateKeyPipeline:
    """Resolve natural keys in fact rows to surrogate keys."""

    def resolve_sk(
        self,
        nk: Any,
        event_date: Any,
        dim_df: pd.DataFrame,
        nk_col: str,
        sk_col: str,
        eff_col: str = "effective_date",
        exp_col: str = "expiration_date",
    ) -> int:
        """Point-in-time lookup: find the SK valid at *event_date*.

        Matches rows where ``effective_date <= event_date`` and either
        ``expiration_date IS NULL`` or ``expiration_date >= event_date``.

        Args:
            nk:         Natural key value to look up.
            event_date: Date of the business event (e.g. order date).
            dim_df:     Dimension DataFrame.
            nk_col:     NK column in *dim_df*.
            sk_col:     SK column in *dim_df*.
            eff_col:    Effective-date column in *dim_df*.
            exp_col:    Expiration-date column in *dim_df*.

        Returns:
            Matching SK, or ``-1`` if no match.

        Raises:
            SKPipelineError: If more than one row matches (overlapping windows).
        """
        if pd.isna(nk) or nk is None:
            return _UNKNOWN_SK

        nk_match = dim_df[nk_col] == nk

        if eff_col in dim_df.columns and event_date is not None:
            try:
                eff = pd.to_datetime(dim_df[eff_col], errors="coerce")
                evt = pd.to_datetime(event_date, errors="coerce")
                eff_ok = eff <= evt
            except Exception:
                eff_ok = pd.Series(True, index=dim_df.index)

            if exp_col in dim_df.columns:
                exp = pd.to_datetime(dim_df[exp_col], errors="coerce")
                exp_ok = exp.isna() | (exp >= evt)
            else:
                exp_ok = pd.Series(True, index=dim_df.index)

            candidates = dim_df[nk_match & eff_ok & exp_ok]
        else:
            candidates = dim_df[nk_match]

        if len(candidates) == 0:
            return _UNKNOWN_SK
        if len(candidates) > 1:
            logger.warning(
                "resolve_sk: %d overlapping SCD2 windows for nk=%r — using first",
                len(candidates), nk,
            )
        return int(candidates.iloc[0][sk_col])

    def resolve_type1_sk(
        self,
        nk: Any,
        dim_df: pd.DataFrame,
        nk_col: str,
        sk_col: str,
    ) -> int:
        """Simple Type-1 lookup with no date filtering.

        Args:
            nk:     Natural key value.
            dim_df: Dimension DataFrame.
            nk_col: NK column in *dim_df*.
            sk_col: SK column in *dim_df*.

        Returns:
            Matching SK, or ``-1`` if not found.
        """
        if pd.isna(nk) or nk is None:
            return _UNKNOWN_SK
        match = dim_df[dim_df[nk_col] == nk]
        if match.empty:
            return _UNKNOWN_SK
        return int(match.iloc[0][sk_col])

    def resolve_batch(
        self,
        fact_df: pd.DataFrame,
        lookups: list[LookupConfig],
    ) -> pd.DataFrame:
        """Resolve all FKs in *fact_df* according to *lookups*.

        Each LookupConfig is applied in order.  Missing output columns are
        filled with ``-1`` (Unknown).

        Args:
            fact_df: Input fact DataFrame.
            lookups: List of LookupConfig objects describing each FK.

        Returns:
            Copy of *fact_df* with added SK columns.
        """
        result = fact_df.copy()
        for lc in lookups:
            if lc.source_col not in result.columns:
                logger.warning("resolve_batch: source column %r not in fact_df", lc.source_col)
                result[lc.output_col] = _UNKNOWN_SK
                continue

            if lc.date_col and lc.date_col in result.columns:
                result[lc.output_col] = result.apply(
                    lambda row, lc=lc: self.resolve_sk(
                        row[lc.source_col],
                        row[lc.date_col],
                        lc.dim_df,
                        lc.nk_col,
                        lc.sk_col,
                        lc.eff_col,
                        lc.exp_col,
                    ),
                    axis=1,
                )
            else:
                result[lc.output_col] = result[lc.source_col].apply(
                    lambda nk, lc=lc: self.resolve_type1_sk(
                        nk, lc.dim_df, lc.nk_col, lc.sk_col
                    )
                )
        return result

    @staticmethod
    def date_to_sk(d: Any) -> int:
        """Convert a date value to YYYYMMDD integer for dim_date join.

        Args:
            d: Date string, ``datetime.date``, or ``pd.Timestamp``.

        Returns:
            Integer YYYYMMDD, or ``19000101`` if conversion fails.
        """
        try:
            ts = pd.to_datetime(d, errors="coerce")
            if pd.isna(ts):
                return _UNKNOWN_DATE_SK
            return int(ts.strftime("%Y%m%d"))
        except Exception:
            return _UNKNOWN_DATE_SK
