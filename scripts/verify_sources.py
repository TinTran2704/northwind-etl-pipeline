"""
scripts/verify_sources.py
Kiểm tra tất cả URLs nguồn dữ liệu có hoạt động không.
Chạy: python scripts/verify_sources.py
"""
import sys
from pathlib import Path

try:
    import requests
except ImportError:
    print("[ERROR] requests chưa cài. Chạy: pip install requests")
    sys.exit(1)

URLS = [
    ("Northwind customers",          "https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data/customers.csv"),
    ("Northwind orders",             "https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data/orders.csv"),
    ("Northwind order-details",      "https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data/order-details.csv"),
    ("Northwind products",           "https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data/products.csv"),
    ("Northwind categories",         "https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data/categories.csv"),
    ("Northwind suppliers",          "https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data/suppliers.csv"),
    ("Northwind employees",          "https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data/employees.csv"),
    ("Northwind territories",        "https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data/territories.csv"),
    ("Northwind employee-terr.",     "https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data/employee-territories.csv"),
    ("REST Countries API",           "https://restcountries.com/v3.1/all?fields=name,cca2"),
    ("Exchange Rate API",            "https://open.er-api.com/v6/latest/USD"),
]


def check(label: str, url: str) -> bool:
    try:
        r = requests.head(url, allow_redirects=True, timeout=10)
        ok = r.status_code == 200
        status = f"OK ({r.status_code})" if ok else f"FAIL ({r.status_code})"
        print(f"  {'✓' if ok else '✗'}  {status:<15}  {label}")
        return ok
    except Exception as e:
        print(f"  ✗  ERROR          {label} — {e}")
        return False


def main():
    print("\n=== Verifying Data Sources ===\n")
    results = [check(label, url) for label, url in URLS]
    ok = sum(results)
    fail = len(results) - ok
    print(f"\nResult: {ok}/{len(results)} OK")
    if fail > 0:
        print(f"[WARN] {fail} nguồn không khả dụng — dùng fallback data/seed/ hoặc kiểm tra mạng")
        sys.exit(1)
    else:
        print("✓ Tất cả nguồn khả dụng!")


if __name__ == "__main__":
    main()