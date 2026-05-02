import duckdb
from src.config import WAREHOUSE_PATH


def validate_warehouse():
    con = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)
    checks = []

    census_count = con.execute("SELECT COUNT(*) FROM census").fetchone()[0]
    checks.append(("census_tract_count", census_count, census_count > 70000))

    dup_count = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT tract_fips, COUNT(*) as n FROM census GROUP BY tract_fips HAVING n > 1
        )
    """).fetchone()[0]
    checks.append(("census_no_duplicate_fips", dup_count, dup_count == 0))

    cdc_join = con.execute("""
        SELECT COUNT(*) FROM master_tract WHERE casthma IS NOT NULL
    """).fetchone()[0]
    cdc_rate = cdc_join / census_count if census_count else 0
    checks.append(("cdc_join_rate", f"{cdc_rate:.1%}", cdc_rate > 0.5))

    tri_assigned = con.execute("""
        SELECT COUNT(*) FROM tri_facilities WHERE tract_fips IS NOT NULL
    """).fetchone()[0]
    tri_total = con.execute("SELECT COUNT(*) FROM tri_facilities").fetchone()[0]
    tri_rate = tri_assigned / tri_total if tri_total else 0
    checks.append(("tri_geocode_rate", f"{tri_rate:.1%}", tri_rate > 0.8))

    con.close()

    print("\n=== Data Quality Report ===")
    all_pass = True
    for name, value, passed in checks:
        status = "PASS" if passed else "FAIL"
        if not passed:
            all_pass = False
        print(f"  [{status}] {name}: {value}")

    return all_pass
