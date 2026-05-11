# 05 - Extract Phase

## 5.1 Mục tiêu

- Lấy dữ liệu từ source về `data/raw/` ở dạng **immutable snapshot**.
- Mỗi lần chạy tạo 1 thư mục `data/raw/{source}/{YYYY-MM-DD-HHMMSS}/`.
- Ghi nhận metadata vào `dim_audit` (extract phase).

## 5.2 Module structure (`src/extract/`)

```
src/extract/
├── __init__.py
├── base.py                # BaseExtractor abstract class
├── http_csv_extractor.py  # CSV qua HTTP
├── rest_json_extractor.py # JSON qua REST
├── dirty_data_generator.py # Inject lỗi cho test
└── runner.py              # Orchestrator: đọc config, gọi từng extractor
```

## 5.3 BaseExtractor interface

```python
class BaseExtractor(ABC):
    """
    Interface cho mọi extractor. Tuân thủ Kimball Subsystem #3.
    """
    
    @abstractmethod
    def extract(self) -> ExtractResult: ...
    
    def get_snapshot_path(self) -> Path: ...
    def write_audit_record(self, result: ExtractResult) -> None: ...

@dataclass
class ExtractResult:
    source_name: str
    file_name: str
    snapshot_path: Path
    row_count: int
    byte_size: int
    extracted_at: datetime
    success: bool
    error_message: Optional[str] = None
```

## 5.4 Change Data Capture (Kimball Subsystem #2)

Northwind không có timestamp đáng tin cậy → áp dụng **CRC-based CDC**:

```
1. Extract full file → data/raw/northwind/2024-06-25/customers.csv
2. Tính CRC32 cho từng row (concat all columns rồi hash)
3. So sánh với CRC ngày trước (lưu trong data/staging/_crc_index/customers.parquet)
4. Output:
   - data/staging/cdc/customers_inserts.parquet
   - data/staging/cdc/customers_updates.parquet
   - data/staging/cdc/customers_deletes.parquet  (rows trong CRC cũ nhưng không có CRC mới)
```

**Edge case**: First run → mọi row là INSERT.

## 5.5 Quy tắc bắt buộc

| Rule | Lý do |
|---|---|
| Không bao giờ ghi đè `data/raw/` | Bảo toàn audit trail |
| Mọi extract phải atomic | Không để file half-written |
| Network error → retry 3 lần (exponential backoff) | Robustness |
| Validate file size ≠ 0 | Phát hiện sớm fail |
| Check Content-Type khớp expected | Phát hiện CDN sai |
| Lưu raw response headers | Debug |

## 5.6 Pseudo-code: HttpCsvExtractor

```python
def extract(self) -> ExtractResult:
    snapshot_dir = self.get_snapshot_path()
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    
    target_file = snapshot_dir / f"{self.file_name}.csv"
    
    # Step 1: Download with retry
    response = self._fetch_with_retry(self.url, max_retries=3)
    
    # Step 2: Validate
    if response.status_code != 200:
        raise ExtractError(f"HTTP {response.status_code}")
    if len(response.content) == 0:
        raise ExtractError("Empty response")
    
    # Step 3: Atomic write (write to .tmp then rename)
    tmp = target_file.with_suffix(".tmp")
    tmp.write_bytes(response.content)
    tmp.rename(target_file)
    
    # Step 4: Quick row count check
    row_count = self._count_rows(target_file)
    
    # Step 5: Build result
    return ExtractResult(
        source_name=self.source_name,
        file_name=self.file_name,
        snapshot_path=target_file,
        row_count=row_count,
        byte_size=target_file.stat().st_size,
        extracted_at=datetime.utcnow(),
        success=True
    )
```

## 5.7 Output schema (data/raw structure)

```
data/raw/
├── northwind/
│   └── 2024-06-25-103015/
│       ├── customers.csv
│       ├── orders.csv
│       ├── ...
│       └── _manifest.json   # liệt kê các file + checksum
├── countries/
│   └── 2024-06-25-103105/
│       └── countries.json
└── exchange_rate/
    └── 2024-06-25-103120/
        └── usd_rates.json
```

`_manifest.json`:
```json
{
  "extracted_at": "2024-06-25T10:30:15Z",
  "source": "northwind",
  "files": [
    { "name": "customers.csv", "rows": 91, "bytes": 12340, "sha256": "abc..." },
    { "name": "orders.csv",    "rows": 830, "bytes": 89234, "sha256": "def..." }
  ]
}
```

## 5.8 Test cases (cho pytest)

```python
def test_http_csv_extractor_success():
    """URL hợp lệ → tạo file, audit record."""

def test_http_csv_extractor_404():
    """URL sai → raise ExtractError, không tạo file rỗng."""

def test_http_csv_extractor_atomic_write():
    """Crash giữa chừng → không có .csv file (chỉ .tmp)."""

def test_cdc_first_run():
    """Lần đầu → mọi row là INSERT."""

def test_cdc_unchanged_data():
    """Run 2 với cùng input → 0 inserts, 0 updates."""

def test_cdc_detect_update():
    """Đổi 1 cột của 1 row → đúng 1 row trong updates."""

def test_cdc_detect_delete():
    """Row biến mất → xuất hiện trong deletes."""
```

## 5.9 Logging requirements

Mỗi extract job phải log:
```
[EXTRACT] source=northwind file=customers.csv start=10:30:15
[EXTRACT] source=northwind file=customers.csv rows=91 bytes=12340 elapsed=0.4s
[EXTRACT] source=northwind file=customers.csv → snapshot=data/raw/northwind/2024-06-25-103015/
[CDC]     file=customers.csv inserts=2 updates=1 deletes=0
[AUDIT]   batch_id=abc-123 source=northwind status=SUCCESS
```
