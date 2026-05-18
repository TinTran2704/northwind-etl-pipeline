# 11 - Hướng dẫn Prompt Claude Code hiệu quả

> File này là **kim chỉ nam** cho việc tương tác với Claude Code để generate code. Đọc kỹ trước khi bắt đầu.

## 11.1 Nguyên tắc prompt vàng

### 1. Một prompt = một deliverable rõ ràng

❌ **Tệ**: "Build hệ thống ETL"  
✅ **Tốt**: "Implement `src/extract/http_csv_extractor.py` theo spec ở `docs/05-extract-phase.md` mục 5.6, kèm pytest ở `tests/test_http_csv_extractor.py`"

### 2. Luôn refer đến doc

Claude Code có thể **đọc file**. Hãy chỉ chính xác:

> "Đọc `docs/03-logical-data-map.md` mục 3.2 và implement `dim_customer` mapping."

### 3. Đính kèm acceptance criteria

> "Code phải pass các test cases liệt kê ở `docs/05-extract-phase.md` mục 5.8."

### 4. Yêu cầu small, reviewable PRs

> "Chỉ implement subsystem #10 (Surrogate Key Generator). Không động đến file nào khác."

## 11.2 Prompt templates

### Template A: Setup project skeleton (chạy 1 lần đầu)

```
Tôi muốn bạn setup skeleton cho dự án ETL Python.

Đọc:
- README.md (cấu trúc thư mục)
- docs/01-architecture.md (tech stack)

Yêu cầu:
1. Tạo các thư mục theo cấu trúc trong README mục "Cấu trúc thư mục"
2. Tạo requirements.txt với: pandas, requests, pyyaml, duckdb, pytest, python-dateutil, jellyfish (cho fuzzy match)
3. Tạo `src/common/__init__.py`, `src/common/logging_setup.py` (structlog hoặc logging.config với JSON formatter)
4. Tạo `src/common/config.py` (load YAML, validate schema)
5. Tạo `config/sources.yaml` theo nội dung trong docs/02-data-sources.md mục 2.5
6. Tạo `config/quality_rules.yaml` theo docs/06-clean-phase.md mục 6.5
7. Tạo `.gitignore` chuẩn Python + thêm `data/raw/`, `data/staging/`, `data/warehouse/`

KHÔNG implement business logic ở bước này. Chỉ skeleton.
Sau khi xong, chạy `pytest tests/ -v` để confirm setup ổn (test rỗng cũng được).
```

### Template B: Implement một subsystem cụ thể

```
Implement Subsystem #10: Surrogate Key Generator.

Đọc:
- docs/08-deliver-phase.md mục 8.3 (spec)
- docs/09-subsystems.md (vị trí trong kiến trúc)

Tạo:
1. `src/deliver/surrogate_key_generator.py`
   - Class `SurrogateKeyGenerator`
   - Methods: `next_sk(dim_name)`, `reserve_unknown(dim_name)`, `peek(dim_name)`, `reset(dim_name)`
   - State persistence: `data/warehouse/_meta/sk_sequences.json`
   - Thread-safe (dùng filelock hoặc threading.Lock)
   - Atomic write (write to .tmp + rename)

2. `tests/deliver/test_surrogate_key_generator.py` với các cases:
   - test_next_sk_starts_at_1
   - test_next_sk_increments
   - test_unknown_member_returns_minus_one
   - test_state_persists_across_instances
   - test_concurrent_calls_no_duplicates (dùng threading)
   - test_reset_clears_state

Yêu cầu code style:
- Type hints đầy đủ
- Docstrings theo Google style
- Không dùng print, dùng logger
- Raise custom exceptions (định nghĩa SKGeneratorError trong cùng file)

Sau khi xong, chạy `pytest tests/deliver/test_surrogate_key_generator.py -v` và paste kết quả.
```

### Template C: Implement extractor

```
Implement HTTP CSV Extractor (Subsystem #3).

Đọc:
- docs/05-extract-phase.md (toàn bộ)
- docs/02-data-sources.md mục 2.1 (Northwind URLs)

Tạo:
1. `src/extract/base.py` với:
   - `BaseExtractor` (abstract)
   - `ExtractResult` dataclass
   - Custom exception `ExtractError`

2. `src/extract/http_csv_extractor.py` với:
   - Class `HttpCsvExtractor(BaseExtractor)`
   - Constructor nhận: source_name, base_url, file_name, target_dir
   - Method `extract()` theo pseudo-code mục 5.6
   - Retry logic: 3 lần, exponential backoff (1s, 2s, 4s) — dùng tenacity hoặc tự code
   - Atomic write: .tmp → rename
   - Manifest writer (cập nhật `_manifest.json` trong snapshot dir)

3. `tests/extract/test_http_csv_extractor.py`:
   - Mock `requests.get` bằng `responses` library hoặc `unittest.mock`
   - Cover các test cases ở docs/05-extract-phase.md mục 5.8
   - **Không** thực sự gọi network trong test

Sau khi pass test, chạy thử với URL thật:
```python
ext = HttpCsvExtractor(
    source_name="northwind",
    base_url="https://raw.githubusercontent.com/graphql-compose/graphql-compose-examples/master/examples/northwind/data/csv",
    file_name="customers",
    target_dir=Path("data/raw/northwind")
)
result = ext.extract()
print(result)
```
Paste output.
```

### Template D: Implement screen

```
Implement Column Property Screen (part of Subsystem #4).

Đọc:
- docs/06-clean-phase.md mục 6.3, 6.4, 6.5

Tạo:
1. `src/clean/screens/base_screen.py`:
   - `ScreenResult` dataclass
   - `BaseScreen` abstract
   - Helper `Severity` enum

2. `src/clean/screens/column_property_screen.py`:
   - Class `ColumnPropertyScreen(BaseScreen)`
   - Support rules: not_null, max_length, min_length, in_list, regex, numeric_range, date_range, unique
   - Method `from_config(entity_name)` để load từ quality_rules.yaml
   - Method `check(df)` trả về `List[ScreenResult]`

3. `tests/clean/test_column_property_screen.py`:
   - Mỗi rule type: 1 test pass + 1 test fail
   - Test config loading
   - Test với DataFrame thực từ `data/raw/northwind/.../customers.csv`

Yêu cầu:
- Mỗi violation = 1 ScreenResult với đầy đủ thông tin (record_id, column, expected, actual, message)
- KHÔNG modify input dataframe
- KHÔNG raise exception khi check fail (chỉ collect results)
```

### Template E: Implement SCD Type 2

```
Implement SCD Manager (Subsystem #9).

Đọc:
- docs/04-dimensional-model.md mục 4.6 (SCD logic & ví dụ)
- docs/08-deliver-phase.md mục 8.4 (algorithm)

Tạo:
1. `src/deliver/scd_manager.py`:
   - Class `SCDManager`
   - Method `apply_scd_type_2(new_rows, existing_dim, type2_cols, type1_cols, effective_date)`
     → Returns updated dim DataFrame
   - Method `apply_scd_type_1(new_rows, existing_dim, columns)`
   - Phải dùng SurrogateKeyGenerator (inject qua constructor)

2. `tests/deliver/test_scd_manager.py`:
   - Cover các test cases ở docs/08-deliver-phase.md mục 8.10 (phần SCD)
   - Đặc biệt:
     * test_first_load_inserts_all
     * test_no_change_no_new_row
     * test_type2_change_creates_new_row_and_expires_old
     * test_type1_change_updates_in_place_for_all_versions
     * test_combined_type1_and_type2_change
     * test_no_overlap_periods (invariant)
     * test_deleted_nk_marked_as_expired
     * test_resurrected_nk_creates_new_row

Yêu cầu:
- Không in-place modify input
- Trả về tuple (updated_dim_df, change_log_df) để audit
- change_log_df có cols: customer_nk, action (INSERT/UPDATE_T1/UPDATE_T2_EXPIRE/UPDATE_T2_NEW), old_sk, new_sk
```

### Template F: End-to-end smoke test

```
Tạo end-to-end pipeline runner và smoke test.

Đọc tất cả file docs/01 → docs/10.

Tạo:
1. `src/orchestration/pipeline.py`:
   - Class `Pipeline` orchestrate: extract → clean → conform → deliver
   - Method `run(batch_id=None)`: 
     - Generate batch_id nếu None (UUID)
     - Run từng phase tuần tự
     - Update metadata.runs
     - On failure: log + checkpoint, không cleanup partial data
   - CLI entry point: `python -m src.orchestration.pipeline run`

2. `tests/integration/test_e2e_pipeline.py`:
   - Mock các URL bằng local fixture file (đã download sẵn vào `tests/fixtures/`)
   - Chạy full pipeline
   - Assert:
     * dim_customer có đúng 91 rows (Northwind có 91 customers)
     * fact_sales có ~2155 rows
     * Mọi fact row có audit_sk hợp lệ
     * Mọi customer_sk trong fact_sales tồn tại trong dim_customer
     * Sum(fact_sales.net_amount) > 0

3. Một script bash `scripts/run_pipeline.sh`:
   - Activate venv
   - Set PYTHONPATH
   - Run pipeline
   - Tail log

Sau khi pass test, chạy thật với data live và paste output cuối cùng:
- Số rows ở mỗi dim, fact
- Quality score
- Tổng thời gian
```

## 11.3 Anti-patterns (đừng làm)

| Anti-pattern | Tại sao tệ |
|---|---|
| "Code toàn bộ ETL trong 1 prompt" | Quá nhiều thứ, Claude sẽ skip details, không test được |
| "Generate code không cần test" | Mất khả năng verify, debug rất tốn thời gian sau này |
| "Bắt Claude tự design schema" | Spec đã có sẵn ở `docs/`, đừng để Claude reinvent |
| "Chỉ paste error log không kèm context" | Claude sẽ đoán mò; hãy kèm cả file & doc liên quan |
| "Đổi spec giữa chừng mà không update doc" | Drift giữa code và doc → bug khó tìm |

## 11.4 Workflow đề xuất

### Day 1: Setup
1. Template A → setup skeleton
2. Verify cấu trúc + run pytest

### Day 2: Common utilities
3. "Implement `src/common/logging_setup.py`, `src/common/config.py`, `src/common/metadata.py` theo docs/10. Test coverage > 80%."

### Day 3-4: Extract
4. Template C → HTTP CSV
5. "Implement `RestJsonExtractor` tương tự `HttpCsvExtractor`."
6. "Implement CDC module per docs/05 mục 5.4."

### Day 5: Clean
7. Template D → ColumnProperty
8. Lặp lại cho Structure, DataRule, Reasonability screens
9. "Implement Error Event Logger và Audit Dimension Builder theo docs/06."

### Day 6: Conform
10. "Implement Standardizer theo docs/07 mục 7.3."
11. "Implement Deduplicator theo docs/07 mục 7.4. Dùng `jellyfish.jaro_winkler_similarity`."
12. "Implement Survivorship Selector theo docs/07 mục 7.5."

### Day 7-8: Deliver
13. Template B → SK Generator
14. Template E → SCD Manager
15. "Implement Surrogate Key Pipeline theo docs/08 mục 8.5."
16. "Implement Fact Table Builder theo docs/08 mục 8.6."

### Day 9: Late arriving + Aggregate
17. "Implement Late Arriving Handler theo docs/08 mục 8.6."
18. "Implement Aggregate Builder theo docs/08 mục 8.8."

### Day 10: Integration
19. Template F → E2E
20. Run, fix bugs, document findings.

## 11.5 Mẹo prompt nâng cao

- **Yêu cầu Claude xác nhận trước khi code**: "Trước khi viết code, tóm tắt cho tôi 5 dòng về cách bạn sẽ implement. Đợi tôi confirm rồi mới code."
- **Yêu cầu Claude review code của chính nó**: "Sau khi viết xong, đóng vai senior reviewer và liệt kê 3 issue tiềm ẩn."
- **Khi gặp bug**, paste cả: (a) đoạn code, (b) test fail message, (c) link đến doc spec, (d) hypothesis của bạn.
- **Yêu cầu git commit message**: "Sau khi code xong, suggest 1 commit message theo Conventional Commits."

## 11.6 Checklist trước khi đóng dự án

- [ ] Mọi module trong Phase 1 (mục 9.7) đã có code + test
- [ ] `pytest tests/ -v` pass 100%
- [ ] E2E pipeline chạy được với live data
- [ ] `data/warehouse/_meta/runs/` có ít nhất 1 successful run
- [ ] dim_customer có ≥ 91 rows
- [ ] fact_sales có ≥ 2150 rows
- [ ] Mọi fact_sales row có `audit_sk IS NOT NULL`
- [ ] Quality score của latest run ≥ 0.95
- [ ] Doc đã update (nếu thay đổi spec)
- [ ] README có hướng dẫn run từ scratch
