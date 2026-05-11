"""
scripts/download_seed.py
Tải Northwind CSV + Countries JSON vào data/seed/
Chạy: python scripts/download_seed.py
"""
import json
import sys
from pathlib import Path

try:
    import requests
except ImportError:
    print("[ERROR] requests chưa cài. Chạy: pip install requests")
    sys.exit(1)

BASE_DIR = Path(__file__).parent.parent
SEED_DIR = BASE_DIR / "data" / "seed"

# ── Northwind CSV ──────────────────────────────────────────────
NORTHWIND_BASE = (
    "https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data"
)
NORTHWIND_FILES = [
    "customers",
    "orders",
    "order-details",
    "products",
    "categories",
    "suppliers",
    "employees",
    "territories",
    "employee-territories",
]

# Shippers từ nguồn dự phòng (graphql-compose)
EXTRA_FILES = {
    "shippers": (
        "https://raw.githubusercontent.com/graphql-compose/"
        "graphql-compose-examples/master/examples/northwind/data/csv/shippers.csv"
    ),
}

# ── Countries JSON ─────────────────────────────────────────────
COUNTRIES_URL = (
    "https://restcountries.com/v3.1/all"
    "?fields=name,cca2,cca3,region,subregion,currencies"
)


def download(url: str, dest: Path, label: str) -> bool:
    try:
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            print(f"  [FAIL] {label} — HTTP {r.status_code}")
            return False
        if len(r.content) == 0:
            print(f"  [FAIL] {label} — empty response")
            return False
        dest.write_bytes(r.content)
        size_kb = dest.stat().st_size / 1024
        print(f"  [OK]   {label:<40} {size_kb:6.1f} KB → {dest.relative_to(BASE_DIR)}")
        return True
    except Exception as e:
        print(f"  [ERR]  {label} — {e}")
        return False


def main():
    print("\n=== Downloading Seed Data ===\n")

    # Northwind
    nw_dir = SEED_DIR / "northwind"
    nw_dir.mkdir(parents=True, exist_ok=True)
    ok = fail = 0

    for name in NORTHWIND_FILES:
        url = f"{NORTHWIND_BASE}/{name}.csv"
        dest = nw_dir / f"{name}.csv"
        if download(url, dest, f"northwind/{name}.csv"):
            ok += 1
        else:
            fail += 1

    for name, url in EXTRA_FILES.items():
        dest = nw_dir / f"{name}.csv"
        if download(url, dest, f"northwind/{name}.csv [extras]"):
            ok += 1
        else:
            print(f"  [SKIP] {name}.csv unavailable — using seed fallback if exists")

    # Countries
    countries_dir = SEED_DIR / "countries"
    countries_dir.mkdir(parents=True, exist_ok=True)
    dest = countries_dir / "countries.json"
    if download(COUNTRIES_URL, dest, "countries/countries.json"):
        # Validate JSON
        try:
            data = json.loads(dest.read_text(encoding="utf-8"))
            print(f"         → {len(data)} countries loaded")
            ok += 1
        except json.JSONDecodeError as e:
            print(f"  [ERR]  countries.json is not valid JSON: {e}")
            fail += 1
    else:
        fail += 1

    print(f"\nResult: {ok} OK  |  {fail} FAILED")

    if fail > 0:
        print("\n[HINT] Nếu có lỗi mạng, kiểm tra kết nối hoặc retry sau.")
        print("       Bạn có thể tải thủ công và đặt vào data/seed/northwind/ và data/seed/countries/")
        sys.exit(1)
    else:
        print("\n✓ Seed data sẵn sàng. Bắt đầu phát triển ETL!")


if __name__ == "__main__":
    main()