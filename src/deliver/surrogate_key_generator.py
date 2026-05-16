"""
Surrogate Key Generator — Kimball Subsystem #10.

Thread-safe, file-persisted sequence generator.
State stored in data/warehouse/_meta/sk_sequences.json.
Atomic write pattern: .tmp → rename.
"""

import json
import logging
import threading
from pathlib import Path

logger = logging.getLogger(__name__)

_DEFAULT_META_DIR = Path("data/warehouse/_meta")


class SKGeneratorError(Exception):
    """Raised on sequence persistence failure."""


class SurrogateKeyGenerator:
    """Generate monotonically-increasing surrogate keys per dimension.

    Args:
        meta_dir: Directory for sk_sequences.json.
    """

    def __init__(self, meta_dir: Path = _DEFAULT_META_DIR) -> None:
        self._meta_dir = meta_dir
        self._seq_path = meta_dir / "sk_sequences.json"
        self._lock = threading.Lock()
        meta_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def next_sk(self, dim_name: str) -> int:
        """Return the next SK for *dim_name* and persist the updated sequence.

        SKs start at 1 and never decrease.  Never returns 0 or negative values.

        Args:
            dim_name: Logical dimension name (e.g. ``"dim_customer"``).

        Returns:
            Next integer surrogate key ≥ 1.
        """
        with self._lock:
            seq = self._load()
            seq[dim_name] = seq.get(dim_name, 0) + 1
            self._save(seq)
            return seq[dim_name]

    def batch_next_sk(self, dim_name: str, n: int) -> list[int]:
        """Reserve *n* consecutive SKs for *dim_name* in a single lock.

        Args:
            dim_name: Logical dimension name.
            n:        Number of SKs to reserve.

        Returns:
            List of *n* sequential integers.
        """
        if n <= 0:
            return []
        with self._lock:
            seq = self._load()
            start = seq.get(dim_name, 0) + 1
            seq[dim_name] = start + n - 1
            self._save(seq)
        return list(range(start, start + n))

    @staticmethod
    def reserve_unknown() -> int:
        """Return the reserved SK for the 'Unknown' dimension member.

        Returns:
            Always ``-1``.
        """
        return -1

    def current_max(self, dim_name: str) -> int:
        """Return the last issued SK for *dim_name* (0 if none yet).

        Args:
            dim_name: Logical dimension name.
        """
        return self._load().get(dim_name, 0)

    def reset(self, dim_name: str) -> None:
        """Reset the sequence for *dim_name* to 0 (for testing only).

        Args:
            dim_name: Logical dimension name.
        """
        with self._lock:
            seq = self._load()
            seq.pop(dim_name, None)
            self._save(seq)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load(self) -> dict[str, int]:
        if not self._seq_path.exists():
            return {}
        try:
            return json.loads(self._seq_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("sk_sequences.json unreadable (%s) — starting fresh", exc)
            return {}

    def _save(self, seq: dict[str, int]) -> None:
        tmp = self._seq_path.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(seq, indent=2), encoding="utf-8")
            tmp.replace(self._seq_path)
        except OSError as exc:
            tmp.unlink(missing_ok=True)
            raise SKGeneratorError(f"Failed to persist SK sequences: {exc}") from exc
