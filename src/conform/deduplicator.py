"""
Deduplicator — Kimball Subsystem #7.

Probabilistic record linkage using Jaro-Winkler similarity.
Two-pass strategy: exact match first (O(n)), then fuzzy match (O(n²) on
remaining unlinked records).

Output: a cluster DataFrame with columns [cluster_id, {nk_column}].
"""

import logging
from typing import Any, Optional

import jellyfish
import pandas as pd

logger = logging.getLogger(__name__)

# NK column per entity (natural key used to identify records).
_ENTITY_NK: dict[str, str] = {
    "customers":  "customerID",
    "employees":  "employeeID",
    "products":   "productID",
    "suppliers":  "supplierID",
    "categories": "categoryID",
    "shippers":   "shipperID",
    "orders":     "orderID",
}

# Matching fields + weights per entity (must sum to 1.0).
_ENTITY_MATCH_FIELDS: dict[str, dict[str, float]] = {
    "customers": {
        "companyName": 0.40,
        "phone":       0.20,
        "address":     0.20,
        "city":        0.10,
        "country":     0.10,
    },
    "employees": {
        "lastName":  0.40,
        "firstName": 0.30,
        "city":      0.20,
        "country":   0.10,
    },
    "products": {
        "productName": 0.70,
        "unitPrice":   0.30,
    },
    "suppliers": {
        "companyName": 0.50,
        "city":        0.30,
        "country":     0.20,
    },
}

_DEFAULT_MATCH_FIELDS: dict[str, float] = {"_fallback": 1.0}


class DeduplicatorError(Exception):
    """Raised when deduplication cannot proceed."""


class Deduplicator:
    """Find duplicate records and group them into clusters.

    Args:
        threshold: Minimum similarity score to consider two records a match.
    """

    def __init__(self, threshold: float = 0.85) -> None:
        self.threshold = threshold

    def match_score(
        self,
        a: dict[str, Any],
        b: dict[str, Any],
        fields: list[str],
        weights: dict[str, float],
    ) -> float:
        """Compute a weighted Jaro-Winkler similarity score between records.

        Args:
            a:       First record as a dict.
            b:       Second record as a dict.
            fields:  List of field names to compare.
            weights: Dict mapping field name → weight (must sum to 1.0).

        Returns:
            Similarity score in [0.0, 1.0].
        """
        score = 0.0
        for field in fields:
            val_a = str(a.get(field) or "").strip()
            val_b = str(b.get(field) or "").strip()
            if not val_a and not val_b:
                sim = 1.0
            elif not val_a or not val_b:
                sim = 0.0
            else:
                sim = jellyfish.jaro_winkler_similarity(val_a.lower(), val_b.lower())
            score += weights.get(field, 0.0) * sim
        return round(score, 6)

    def find_clusters(
        self,
        df: pd.DataFrame,
        entity: str,
        threshold: Optional[float] = None,
    ) -> pd.DataFrame:
        """Assign each record to a cluster (group of duplicates).

        Uses a two-pass algorithm:
        1. **Exact-match pass**: records sharing the same primary match field
           value (e.g., exact ``companyName``) are grouped immediately.
        2. **Fuzzy-match pass**: remaining unlinked records are compared
           pairwise; pairs exceeding *threshold* are merged.

        For clean data (like stock Northwind) every record ends up in its
        own single-member cluster.

        Args:
            df:        Input DataFrame with an NK column.
            entity:    Entity name key (e.g. ``"customers"``).
            threshold: Override the instance-level threshold.

        Returns:
            DataFrame with columns ``[cluster_id, {nk_column}]``.

        Raises:
            DeduplicatorError: If NK column is not present in *df*.
        """
        thr = threshold if threshold is not None else self.threshold
        nk_col = _ENTITY_NK.get(entity)
        if nk_col is None or nk_col not in df.columns:
            # Fallback: use first column as NK
            nk_col = df.columns[0]
            logger.warning("find_clusters: no NK mapping for %r, using %r", entity, nk_col)

        weights = _ENTITY_MATCH_FIELDS.get(entity, {})
        match_fields = list(weights.keys())

        records = df.to_dict(orient="records")
        n = len(records)

        # Union-Find for cluster assignment
        parent = list(range(n))

        def _find(i: int) -> int:
            while parent[i] != i:
                parent[i] = parent[parent[i]]
                i = parent[i]
            return i

        def _union(i: int, j: int) -> None:
            ri, rj = _find(i), _find(j)
            if ri != rj:
                parent[ri] = rj

        if match_fields:
            # Pass 1 — exact match on primary field
            primary = match_fields[0]
            exact_groups: dict[str, list[int]] = {}
            for idx, rec in enumerate(records):
                key = str(rec.get(primary) or "").strip().lower()
                if key:
                    exact_groups.setdefault(key, []).append(idx)
            for group in exact_groups.values():
                for j in range(1, len(group)):
                    _union(group[0], group[j])

            # Pass 2 — fuzzy match on remaining unlinked records
            # Find records that are sole members of their cluster after pass 1
            cluster_members: dict[int, list[int]] = {}
            for idx in range(n):
                root = _find(idx)
                cluster_members.setdefault(root, []).append(idx)
            singletons = [
                members[0]
                for members in cluster_members.values()
                if len(members) == 1
            ]
            for i in range(len(singletons)):
                for j in range(i + 1, len(singletons)):
                    idx_i, idx_j = singletons[i], singletons[j]
                    if _find(idx_i) == _find(idx_j):
                        continue
                    score = self.match_score(
                        records[idx_i], records[idx_j], match_fields, weights
                    )
                    if score >= thr:
                        _union(idx_i, idx_j)
                        logger.debug(
                            "find_clusters: merged %r ↔ %r (score=%.3f)",
                            records[idx_i].get(nk_col),
                            records[idx_j].get(nk_col),
                            score,
                        )

        # Build output DataFrame
        root_to_cluster: dict[int, str] = {}
        cluster_counter = 0
        rows = []
        for idx in range(n):
            root = _find(idx)
            if root not in root_to_cluster:
                cluster_counter += 1
                root_to_cluster[root] = f"cluster_{cluster_counter:06d}"
            rows.append({
                "cluster_id": root_to_cluster[root],
                nk_col: records[idx].get(nk_col),
            })

        result = pd.DataFrame(rows, columns=["cluster_id", nk_col])
        duplicates = cluster_counter < n
        if duplicates:
            logger.info(
                "find_clusters: entity=%s records=%d clusters=%d duplicates_detected=%d",
                entity, n, cluster_counter, n - cluster_counter,
            )
        return result
