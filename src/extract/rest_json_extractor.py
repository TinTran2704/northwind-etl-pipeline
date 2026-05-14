"""
REST JSON extractor. Implements Kimball Subsystem #3.

Downloads a JSON endpoint, validates the payload, writes atomically to
the snapshot directory. Falls back to a seed file when the API is down.
See docs/05-extract-phase.md and docs/02-data-sources.md §2.2.
"""

import hashlib
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

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


class RestJsonExtractor(BaseExtractor):
    """Extract a JSON REST endpoint to a snapshot file.

    Features:
    - Retry up to 3 times on network errors (1 s, 2 s, 4 s backoff).
    - JSON validation before write (rejects invalid payloads).
    - Atomic write via ``.tmp`` → rename.
    - Fallback to ``fallback_seed`` when the API is unreachable after retries.

    Args:
        source_name:   Logical source identifier, e.g. ``"countries"``.
        url:           Full endpoint URL.
        file_name:     Output file stem without extension, e.g. ``"countries"``.
        target_dir:    Root snapshot directory.
        fallback_seed: Optional path to a seed file used when the API fails.
    """

    def __init__(
        self,
        source_name: str,
        url: str,
        file_name: str,
        target_dir: Path,
        fallback_seed: Optional[Path] = None,
    ) -> None:
        super().__init__(source_name, target_dir)
        self.url = url
        self.file_name = file_name
        self.fallback_seed = fallback_seed

    def extract(self) -> ExtractResult:
        """Fetch JSON, validate, write atomically, return result.

        Raises:
            ExtractError: On HTTP 4xx, invalid JSON, or missing fallback.
        """
        started_at = datetime.utcnow()
        snapshot_dir = self.get_snapshot_path()
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        target_file = snapshot_dir / f"{self.file_name}.json"

        logger.info(
            "[EXTRACT] source=%s file=%s.json start=%s",
            self.source_name, self.file_name,
            started_at.strftime("%H:%M:%S"),
        )

        content = self._get_content()

        row_count = self._count_rows(content)
        sha256 = hashlib.sha256(content).hexdigest()

        # Atomic write
        tmp = target_file.with_suffix(".tmp")
        try:
            tmp.write_bytes(content)
            tmp.rename(target_file)
        except Exception:
            tmp.unlink(missing_ok=True)
            raise

        byte_size = target_file.stat().st_size
        elapsed = (datetime.utcnow() - started_at).total_seconds()

        logger.info(
            "[EXTRACT] source=%s file=%s.json rows=%d bytes=%d elapsed=%.2fs",
            self.source_name, self.file_name, row_count, byte_size, elapsed,
        )
        logger.info(
            "[EXTRACT] source=%s file=%s.json → snapshot=%s",
            self.source_name, self.file_name, snapshot_dir,
        )

        return ExtractResult(
            source_name=self.source_name,
            file_name=self.file_name,
            snapshot_path=target_file,
            row_count=row_count,
            byte_size=byte_size,
            extracted_at=started_at,
            success=True,
        )

    def _get_content(self) -> bytes:
        """Fetch content from URL; fall back to seed if URL fails after retries."""
        try:
            response = self._fetch(self.url)
            content = response.content
        except (ExtractError, RetryError, requests.RequestException) as exc:
            if self.fallback_seed and self.fallback_seed.exists():
                logger.warning(
                    "[EXTRACT] source=%s URL failed (%s) — using seed fallback: %s",
                    self.source_name, exc, self.fallback_seed,
                )
                content = self.fallback_seed.read_bytes()
            else:
                raise ExtractError(
                    f"All retries exhausted for {self.url} and no seed fallback: {exc}"
                ) from exc

        self._validate_json(content)
        return content

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=4),
        retry=retry_if_exception_type(requests.RequestException),
    )
    def _fetch(self, url: str) -> requests.Response:
        """GET url with retry on network errors and 5xx.

        After 3 failures tenacity raises RetryError (caught in _get_content()).
        """
        response = requests.get(url, timeout=30)
        if response.status_code >= 500:
            raise requests.HTTPError(
                f"HTTP {response.status_code}", response=response
            )
        if response.status_code != 200:
            raise ExtractError(f"HTTP {response.status_code} for {url}")
        if not response.content:
            raise ExtractError(f"Empty response from {url}")
        return response

    def _validate_json(self, content: bytes) -> Any:
        """Parse content as JSON; raise ExtractError if invalid.

        Returns:
            Parsed JSON (list or dict).
        """
        try:
            return json.loads(content)
        except json.JSONDecodeError as exc:
            raise ExtractError(
                f"Invalid JSON from {self.url}: {exc}"
            ) from exc

    def _count_rows(self, content: bytes) -> int:
        """Count rows: length if top-level is list, else 1."""
        data = json.loads(content)
        return len(data) if isinstance(data, list) else 1
