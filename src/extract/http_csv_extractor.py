"""
HTTP CSV extractor. Implements Kimball Subsystem #3.

Downloads a single CSV file from an HTTP endpoint, writes it atomically
to the snapshot directory, and updates _manifest.json.
See docs/05-extract-phase.md §5.4-5.6.
"""

import hashlib
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.extract.base import BaseExtractor, ExtractError, ExtractResult

logger = logging.getLogger(__name__)


class HttpCsvExtractor(BaseExtractor):
    """Extract a single CSV file from an HTTP URL.

    Features:
    - Retry up to 3 times on network errors (1 s, 2 s, 4 s backoff).
    - Atomic write via ``.tmp`` → rename (no half-written files).
    - SHA-256 checksum written to ``_manifest.json``.
    - 5xx responses are retried; 4xx raise ExtractError immediately.

    Args:
        source_name: Logical source identifier, e.g. ``"northwind"``.
        base_url:    Base HTTP URL without trailing slash.
        file_name:   File stem without extension, e.g. ``"customers"``.
        target_dir:  Root snapshot directory, e.g. ``Path("data/raw/northwind")``.
    """

    def __init__(
        self,
        source_name: str,
        base_url: str,
        file_name: str,
        target_dir: Path,
    ) -> None:
        super().__init__(source_name, target_dir)
        self.base_url = base_url.rstrip("/")
        self.file_name = file_name

    @property
    def url(self) -> str:
        return f"{self.base_url}/{self.file_name}.csv"

    def extract(self) -> ExtractResult:
        """Download CSV, write atomically, update manifest, return result.

        Raises:
            ExtractError: On HTTP 4xx, empty body, or I/O failure.
        """
        started_at = datetime.utcnow()
        snapshot_dir = self.get_snapshot_path()
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        target_file = snapshot_dir / f"{self.file_name}.csv"

        logger.info(
            "[EXTRACT] source=%s file=%s.csv start=%s",
            self.source_name, self.file_name,
            started_at.strftime("%H:%M:%S"),
        )

        try:
            response = self._fetch(self.url)
        except RetryError as exc:
            raise ExtractError(f"All retries exhausted for {self.url}: {exc}") from exc

        content: bytes = response.content

        # Atomic write: .tmp → rename so readers never see a partial file.
        tmp = target_file.with_suffix(".tmp")
        try:
            tmp.write_bytes(content)
            tmp.rename(target_file)
        except Exception:
            tmp.unlink(missing_ok=True)
            raise

        row_count = self._count_rows(target_file)
        byte_size = target_file.stat().st_size
        sha256 = hashlib.sha256(content).hexdigest()
        elapsed = (datetime.utcnow() - started_at).total_seconds()

        logger.info(
            "[EXTRACT] source=%s file=%s.csv rows=%d bytes=%d elapsed=%.2fs",
            self.source_name, self.file_name, row_count, byte_size, elapsed,
        )
        logger.info(
            "[EXTRACT] source=%s file=%s.csv → snapshot=%s",
            self.source_name, self.file_name, snapshot_dir,
        )

        result = ExtractResult(
            source_name=self.source_name,
            file_name=self.file_name,
            snapshot_path=target_file,
            row_count=row_count,
            byte_size=byte_size,
            extracted_at=started_at,
            success=True,
        )
        self._update_manifest(snapshot_dir, result, sha256)
        return result

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=4),
        retry=retry_if_exception_type(requests.RequestException),
    )
    def _fetch(self, url: str) -> requests.Response:
        """GET url with retry on network errors and 5xx responses.

        4xx raises ExtractError immediately (no retry).
        After 3 network failures tenacity raises RetryError (caught in extract()).
        """
        response = requests.get(url, timeout=30)
        if response.status_code >= 500:
            # Treated as transient — tenacity will retry.
            raise requests.HTTPError(
                f"HTTP {response.status_code}", response=response
            )
        if response.status_code != 200:
            raise ExtractError(f"HTTP {response.status_code} for {url}")
        if not response.content:
            raise ExtractError(f"Empty response from {url}")
        return response

    def _count_rows(self, file_path: Path) -> int:
        # on_bad_lines='skip' handles source CSVs that contain unquoted commas
        # in field values (e.g., Northwind address fields).
        return len(pd.read_csv(file_path, on_bad_lines="skip"))

    def _update_manifest(
        self,
        snapshot_dir: Path,
        result: ExtractResult,
        sha256: str,
    ) -> None:
        manifest_path = snapshot_dir / "_manifest.json"
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        else:
            manifest = {
                "extracted_at": result.extracted_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "source": result.source_name,
                "files": [],
            }
        manifest["files"].append(
            {
                "name": f"{result.file_name}.csv",
                "rows": result.row_count,
                "bytes": result.byte_size,
                "sha256": sha256,
            }
        )
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
