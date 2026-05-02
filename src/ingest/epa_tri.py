import asyncio
import httpx
import pandas as pd
from tqdm import tqdm

from src.config import STATE_FIPS, RAW_DIR
from src.ingest.base import BaseIngestor

EPA_BASE = "https://data.epa.gov/efservice"
BATCH_SIZE = 10000


class EPATRIIngestor(BaseIngestor):
    name = "epa_tri"

    def __init__(self):
        super().__init__()
        self.output_dir = RAW_DIR / "epa_tri"

    def _facility_url(self, state_abbr: str, start: int, end: int) -> str:
        return f"{EPA_BASE}/tri_facility/state_abbr/{state_abbr}/rows/{start}:{end}/json"

    def _release_url(self, state_abbr: str, year: int, start: int, end: int) -> str:
        return (
            f"{EPA_BASE}/tri_reporting_form/reporting_year/{year}"
            f"/state_abbr/{state_abbr}/rows/{start}:{end}/json"
        )

    def fetch_state_facilities(self, state_abbr: str) -> pd.DataFrame:
        import httpx as httpx_sync
        rows = []
        start = 1
        while True:
            url = self._facility_url(state_abbr, start, start + BATCH_SIZE - 1)
            resp = httpx_sync.get(url, timeout=60)
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                break
            rows.extend(batch)
            if len(batch) < BATCH_SIZE:
                break
            start += BATCH_SIZE
        df = pd.DataFrame(rows)
        df["fac_latitude"] = pd.to_numeric(df["fac_latitude"], errors="coerce")
        df["fac_longitude"] = pd.to_numeric(df["fac_longitude"], errors="coerce")
        return df

    def fetch_state_releases(self, state_abbr: str, year: int = 2022) -> pd.DataFrame:
        import httpx as httpx_sync
        rows = []
        start = 1
        while True:
            url = self._release_url(state_abbr, year, start, start + BATCH_SIZE - 1)
            resp = httpx_sync.get(url, timeout=60)
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                break
            rows.extend(batch)
            if len(batch) < BATCH_SIZE:
                break
            start += BATCH_SIZE
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        release_cols = [c for c in df.columns if "release" in c.lower() or "fugitive" in c.lower() or "stack" in c.lower()]
        df = df.rename(columns={
            "cas_chem_name": "chemical",
            "cas_number": "cas_number",
        })
        for col in release_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        if "total_releases" in df.columns:
            df["total_releases_lbs"] = df["total_releases"]
        elif "on_site_release_total" in df.columns and "off_site_release_total" in df.columns:
            df["total_releases_lbs"] = df["on_site_release_total"].fillna(0) + df["off_site_release_total"].fillna(0)
        else:
            numeric_release = [c for c in release_cols if df[c].dtype in ("float64", "int64")]
            df["total_releases_lbs"] = df[numeric_release].sum(axis=1) if numeric_release else 0
        return df

    async def _fetch_paginated(self, client: httpx.AsyncClient, url_fn, **kwargs) -> list[dict]:
        rows = []
        start = 1
        while True:
            url = url_fn(start=start, end=start + BATCH_SIZE - 1, **kwargs)
            try:
                batch = await self._fetch_json(client, url)
            except Exception:
                break
            if not batch:
                break
            rows.extend(batch)
            if len(batch) < BATCH_SIZE:
                break
            start += BATCH_SIZE
        return rows

    async def run(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)
        async with httpx.AsyncClient() as client:
            fac_dfs = []
            for abbr in tqdm(STATE_FIPS.values(), desc="TRI Facilities"):
                rows = await self._fetch_paginated(
                    client,
                    lambda start, end, sa=abbr: self._facility_url(sa, start, end),
                )
                if rows:
                    df = pd.DataFrame(rows)
                    df["fac_latitude"] = pd.to_numeric(df["fac_latitude"], errors="coerce")
                    df["fac_longitude"] = pd.to_numeric(df["fac_longitude"], errors="coerce")
                    fac_dfs.append(df)

            facilities = pd.concat(fac_dfs, ignore_index=True) if fac_dfs else pd.DataFrame()
            self.save_parquet(facilities, "tri_facilities_all.parquet")

            rel_dfs = []
            for abbr in tqdm(STATE_FIPS.values(), desc="TRI Releases 2022"):
                rows = await self._fetch_paginated(
                    client,
                    lambda start, end, sa=abbr: self._release_url(sa, 2022, start, end),
                )
                if rows:
                    rel_dfs.append(pd.DataFrame(rows))

            if rel_dfs:
                releases = pd.concat(rel_dfs, ignore_index=True)
                self.save_parquet(releases, "tri_releases_2022_all.parquet")
            else:
                releases = pd.DataFrame()

        print(f"TRI: {len(facilities)} facilities, {len(releases)} release records")
        return facilities, releases
