import duckdb
import pandas as pd
from pathlib import Path

from src.config import RAW_DIR, WAREHOUSE_PATH, GEO_DIR
from src.join.tract_geo import TractGeocoder


def build_master_table():
    print("Building master table...")

    geocoder = TractGeocoder()
    geocoder.load()

    tri_fac_path = RAW_DIR / "epa_tri" / "tri_facilities_all.parquet"
    if tri_fac_path.exists():
        print("Geocoding TRI facilities...")
        tri_fac = pd.read_parquet(tri_fac_path)
        tri_fac = geocoder.bulk_assign(tri_fac, lat_col="fac_latitude", lon_col="fac_longitude")
        tri_fac.to_parquet(RAW_DIR / "epa_tri" / "tri_facilities_geocoded.parquet", index=False)
        print(f"  {tri_fac['tract_fips'].notna().sum()}/{len(tri_fac)} facilities assigned to tracts")

    ghg_path = RAW_DIR / "epa_ghg" / "ghg_facilities_2022.parquet"
    if ghg_path.exists():
        print("Geocoding GHG facilities...")
        ghg = pd.read_parquet(ghg_path)
        if "latitude" in ghg.columns:
            ghg = geocoder.bulk_assign(ghg, lat_col="latitude", lon_col="longitude")
            ghg.to_parquet(RAW_DIR / "epa_ghg" / "ghg_facilities_geocoded.parquet", index=False)

    print("Building DuckDB warehouse...")
    con = duckdb.connect(str(WAREHOUSE_PATH))

    census_path = RAW_DIR / "census" / "census_all.parquet"
    if census_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE census AS
            SELECT * FROM read_parquet('{census_path}')
        """)
        count = con.execute("SELECT COUNT(*) FROM census").fetchone()[0]
        print(f"  census: {count} tracts")

    cdc_path = RAW_DIR / "cdc_places" / "cdc_places_all.parquet"
    if cdc_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE cdc_places AS
            SELECT * FROM read_parquet('{cdc_path}')
        """)
        count = con.execute("SELECT COUNT(*) FROM cdc_places").fetchone()[0]
        print(f"  cdc_places: {count} tracts")

    tri_geo_path = RAW_DIR / "epa_tri" / "tri_facilities_geocoded.parquet"
    if tri_geo_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE tri_facilities AS
            SELECT * FROM read_parquet('{tri_geo_path}')
        """)

    tri_rel_path = RAW_DIR / "epa_tri" / "tri_releases_2022_all.parquet"
    if tri_rel_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE tri_releases AS
            SELECT * FROM read_parquet('{tri_rel_path}')
        """)

    sdwis_viol_path = RAW_DIR / "epa_sdwis" / "sdwis_violations_all.parquet"
    if sdwis_viol_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE sdwis_violations AS
            SELECT * FROM read_parquet('{sdwis_viol_path}')
        """)

    sdwis_sys_path = RAW_DIR / "epa_sdwis" / "sdwis_systems_all.parquet"
    if sdwis_sys_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE sdwis_systems AS
            SELECT * FROM read_parquet('{sdwis_sys_path}')
        """)

    ghg_geo_path = RAW_DIR / "epa_ghg" / "ghg_facilities_geocoded.parquet"
    if ghg_geo_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE ghg_facilities AS
            SELECT * FROM read_parquet('{ghg_geo_path}')
        """)

    storms_path = RAW_DIR / "noaa_storms" / "noaa_storms_all.parquet"
    if storms_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE noaa_storms AS
            SELECT * FROM read_parquet('{storms_path}')
        """)

    echo_path = RAW_DIR / "epa_echo" / "echo_facilities_all.parquet"
    if echo_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE echo_facilities AS
            SELECT * FROM read_parquet('{echo_path}')
        """)

    con.execute("""
        CREATE OR REPLACE VIEW master_tract AS
        SELECT
            c.*,
            p.*  EXCLUDE (tract_fips),
            tri_agg.tri_facility_count,
            tri_agg.tri_total_releases_lbs
        FROM census c
        LEFT JOIN cdc_places p ON c.tract_fips = p.tract_fips
        LEFT JOIN (
            SELECT
                tract_fips,
                COUNT(*) AS tri_facility_count,
                SUM(CAST(0 AS DOUBLE)) AS tri_total_releases_lbs
            FROM tri_facilities
            WHERE tract_fips IS NOT NULL
            GROUP BY tract_fips
        ) tri_agg ON c.tract_fips = tri_agg.tract_fips
    """)

    master_count = con.execute("SELECT COUNT(*) FROM master_tract").fetchone()[0]
    print(f"\nMaster tract view: {master_count} tracts")

    con.execute(f"""
        COPY (SELECT * FROM master_tract)
        TO '{RAW_DIR / "master_tract.parquet"}' (FORMAT PARQUET)
    """)

    con.close()
    print("Warehouse built successfully")
