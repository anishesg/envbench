import asyncio
import httpx
import pandas as pd
from tqdm import tqdm

from src.config import CDC_MEASURES, STATE_FIPS, RAW_DIR
from src.ingest.base import BaseIngestor

CDC_BASE = "https://data.cdc.gov/resource/cwsq-ngmh.json"
PAGE_SIZE = 50000


class CDCPlacesIngestor(BaseIngestor):
    name = "cdc_places"

    def __init__(self):
        super().__init__()
        self.output_dir = RAW_DIR / "cdc_places"

    def fetch_measure_state(self, measure_id: str, state_abbr: str) -> pd.DataFrame:
        import httpx as httpx_sync
        rows = []
        offset = 0
        while True:
            params = {
                "$limit": PAGE_SIZE,
                "$offset": offset,
                "measureid": measure_id,
                "stateabbr": state_abbr,
                "$select": "locationid,measureid,data_value,low_confidence_limit,high_confidence_limit,totalpopulation,data_value_type",
            }
            resp = httpx_sync.get(CDC_BASE, params=params, timeout=60)
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                break
            rows.extend(batch)
            if len(batch) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df = df.rename(columns={"locationid": "tract_fips", "measureid": "measure_id"})
        df["data_value"] = pd.to_numeric(df["data_value"], errors="coerce")
        df["low_confidence_limit"] = pd.to_numeric(df["low_confidence_limit"], errors="coerce")
        df["high_confidence_limit"] = pd.to_numeric(df["high_confidence_limit"], errors="coerce")
        return df

    def pivot_to_tract_level(self, df: pd.DataFrame) -> pd.DataFrame:
        crude = df[df["data_value_type"] == "Crude prevalence"].copy()
        pivoted = crude.pivot_table(
            index="tract_fips",
            columns="measure_id",
            values="data_value",
            aggfunc="first",
        ).reset_index()
        pivoted.columns = [c.lower() if c != "tract_fips" else c for c in pivoted.columns]
        return pivoted

    async def _fetch_measure_state_async(
        self, client: httpx.AsyncClient, measure_id: str, state_abbr: str
    ) -> pd.DataFrame | None:
        rows = []
        offset = 0
        while True:
            params = {
                "$limit": PAGE_SIZE,
                "$offset": offset,
                "measureid": measure_id,
                "stateabbr": state_abbr,
                "$select": "locationid,measureid,data_value,low_confidence_limit,high_confidence_limit,totalpopulation,data_value_type",
            }
            try:
                data = await self._fetch_json(client, CDC_BASE, params)
            except Exception as e:
                print(f"  FAILED CDC {measure_id}/{state_abbr}: {e}")
                return None
            if not data:
                break
            rows.extend(data)
            if len(data) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
        if not rows:
            return None
        df = pd.DataFrame(rows)
        df = df.rename(columns={"locationid": "tract_fips", "measureid": "measure_id"})
        df["data_value"] = pd.to_numeric(df["data_value"], errors="coerce")
        return df

    async def run(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)
        all_dfs = []
        async with httpx.AsyncClient() as client:
            tasks = []
            combos = [(m, sa) for m in CDC_MEASURES for sa in STATE_FIPS.values()]
            for measure_id, state_abbr in combos:
                tasks.append(self._fetch_measure_state_async(client, measure_id, state_abbr))
            results = []
            for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="CDC PLACES"):
                result = await coro
                if result is not None:
                    results.append(result)
        combined = pd.concat(results, ignore_index=True)
        pivoted = self.pivot_to_tract_level(combined)
        self.save_parquet(pivoted, "cdc_places_all.parquet")
        print(f"CDC PLACES: {len(pivoted)} tracts, {len(CDC_MEASURES)} measures")
        return pivoted
