# CLAUDE.md — Hướng dẫn cho Claude Code

> File này được Claude Code đọc tự động khi vào project. Đây là **single source of truth** về quy ước dự án.

## Bối cảnh dự án

Đây là dự án xây dựng **hệ thống ETL theo phương pháp luận Kimball/Caserta** với 4 giai đoạn `Extract → Clean → Conform → Deliver`. Mục tiêu giáo dục: implement đầy đủ các phân hệ (subsystems) chuẩn của Kimball Group.

**Tech stack:**
- Python 3.11+
- Pandas + DuckDB (storage)
- pytest (testing)
- requests + tenacity (extract)
- jellyfish (fuzzy matching)
- pyyaml (config)
- Apache Kafka (cho streaming ingestion, phase 2)

## ⚠️ Quy tắc tuyệt đối

1. **Đọc spec trước khi code.** Mọi yêu cầu đều có spec trong `docs/`. Đừng tự design — hãy implement theo spec.
2. **Không bao giờ skip tests.** Mỗi module mới phải kèm pytest test. Coverage > 80%.
3. **Không bao giờ commit `data/raw/`, `data/staging/`, `data/warehouse/`** — chỉ commit `data/seed/`.
4. **Không hardcode credentials hoặc URL** trong code business — luôn đọc từ `config/`.
5. **Mọi module phải log với prefix `[batch=...]`** — xem `docs/10-metadata-strategy.md`.
6. **Một PR/commit = một subsystem** — không trộn nhiều thay đổi.

## Cấu trúc tài liệu

| Doc | Khi nào đọc |
|---|---|
| `docs/01-architecture.md` | Trước mọi việc — hiểu Back Room vs Front Room |
| `docs/02-data-sources.md` | Khi implement extract, hoặc cần tra URL nguồn |
| `docs/03-logical-data-map.md` | Khi build dim hoặc fact — đây là contract giữa source và target |
| `docs/04-dimensional-model.md` | Khi cần quyết định về SCD, schema, naming |
| `docs/05-extract-phase.md` | Khi implement extractor, CDC |
| `docs/06-clean-phase.md` | Khi implement screen, error event, audit dim |
| `docs/07-conform-phase.md` | Khi implement standardize, dedupe, survivorship |
| `docs/08-deliver-phase.md` | Khi build SCD manager, SK pipeline, fact builder, aggregate |
| `docs/09-subsystems.md` | Khi cần biết module nào là Subsystem # mấy |
| `docs/10-metadata-strategy.md` | Khi setup logging, lineage, audit |
| `docs/11-prompting-guide.md` | (Cho human user) — hướng dẫn prompt template |

## Chuẩn code style

```python
# Imports: stdlib → third-party → local, mỗi nhóm cách nhau dòng trống
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from pydantic import BaseModel

from src.common.metadata import MetadataContext

logger = logging.getLogger(__name__)


class CustomerExtractor:
    """
    Extract customer data from Northwind source.
    
    Tuân thủ Subsystem #3 (docs/09-subsystems.md).
    
    Args:
        source_url: Base URL of the source.
        target_dir: Output directory for snapshots.
    
    Raises:
        ExtractError: If extraction fails after retries.
    """
    
    def __init__(self, source_url: str, target_dir: Path) -> None:
        self.source_url = source_url
        self.target_dir = target_dir
    
    def extract(self) -> ExtractResult:
        """Extract and persist customer snapshot."""
        ...
```

**Yêu cầu:**
- Type hints **bắt buộc** mọi public function/method.
- Docstring Google style cho mọi class & public method.
- `logger` (không `print`) — module-level `logger = logging.getLogger(__name__)`.
- Custom exceptions trong cùng module (`ExtractError`, `CleanError`, `SCDError`, …).
- Không catch `Exception` chung chung — bắt đúng loại.

## Test pattern

```python
# tests/extract/test_http_csv_extractor.py
import pytest
from unittest.mock import patch, MagicMock
from src.extract.http_csv_extractor import HttpCsvExtractor, ExtractError


class TestHttpCsvExtractor:
    @pytest.fixture
    def extractor(self, tmp_path):
        return HttpCsvExtractor(
            source_name="northwind",
            base_url="https://example.com/data",
            file_name="customers",
            target_dir=tmp_path,
        )
    
    def test_extract_success(self, extractor, requests_mock):
        requests_mock.get(
            "https://example.com/data/customers.csv",
            text="CustomerID,CompanyName\nALFKI,Alfreds\n",
        )
        result = extractor.extract()
        assert result.success is True
        assert result.row_count == 1
    
    def test_extract_404_raises(self, extractor, requests_mock):
        requests_mock.get("https://example.com/data/customers.csv", status_code=404)
        with pytest.raises(ExtractError, match="HTTP 404"):
            extractor.extract()
```

**Yêu cầu:**
- Mock external network bằng `requests_mock` hoặc `responses` — KHÔNG hit network thật trong unit test.
- Dùng `tmp_path` fixture cho file system test, không pollute disk.
- Tên test: `test_<what>_<condition>_<expected>`.

## Tên file & cấu trúc

```
src/
├── common/             # Cross-cutting: logging, config, metadata, types
├── extract/            # Subsystems 1-3
├── clean/              # Subsystems 4-6
├── conform/            # Subsystems 7-8, 17, 21
├── deliver/            # Subsystems 9-16, 18-20
└── orchestration/      # Subsystem 22-31

tests/
└── <mirror src structure>
```

Mỗi `__init__.py` phải có docstring giải thích vai trò của module:
```python
"""
Extract phase modules. Implements Kimball Subsystems 1-3.
See docs/05-extract-phase.md for spec.
"""
```

## Quy trình làm việc

Khi user yêu cầu "implement X":

1. **Đọc** spec ở `docs/`.
2. **Tóm tắt** trong 3-5 dòng cách bạn sẽ implement, **chờ** user confirm trước khi code (trừ khi user đã yêu cầu rõ "code luôn").
3. **Implement** code + test cùng lúc.
4. **Chạy** `pytest tests/<path> -v` và paste output.
5. **Đề xuất** commit message theo Conventional Commits: `feat(extract): add HttpCsvExtractor`.

## Khi gặp ambiguity

Nếu spec không rõ:
1. Đọc lại `docs/` — có thể đã có ở đó.
2. Nếu thật sự không có → **hỏi user**, đừng tự quyết.
3. Sau khi user trả lời → đề nghị **update spec** trước khi code.

## Anti-patterns cần tránh

- ❌ Code mà không đọc `docs/`
- ❌ Implement nhiều subsystem trong 1 file
- ❌ Skip test "vì đã chắc chắn đúng"
- ❌ Catch `except Exception:` không log gì
- ❌ Hardcode URL, path, magic number
- ❌ In-place mutation của input DataFrame
- ❌ Đổi spec mà không update doc

## Workflow check

Trước khi báo "done":
- [ ] Code có type hints
- [ ] Có docstring
- [ ] Có test
- [ ] `pytest` pass
- [ ] `pyright` hoặc `mypy` không error nghiêm trọng
- [ ] Không hardcode
- [ ] Đã log đủ thông tin
- [ ] Doc đã update (nếu có thay đổi spec)
