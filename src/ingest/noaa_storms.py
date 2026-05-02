import asyncio
import gzip
import httpx
import pandas as pd
from io import BytesIO
from tqdm import tqdm

from src.config import RAW_DIR
from src.ingest.base import BaseIngestor

STORM_BASE = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles"


class NOAAStormIngestor(BaseIngestor):
    name = "noaa_storms"

    def __init__(self, years: range = range(2015, 2025)):
        super().__init__()
        self.output_dir = RAW_DIR / "noaa_storms"
        self.years = years

    async def _fetch_year(self, client: httpx.AsyncClient, year: int) -> pd.DataFrame | None:
        filename = f"StormEvents_details-ftp_v1.0_d{year}_c20240620.csv.gz"
        url = f"{STORM_BASE}/{filename}"
        try:
            async with self._semaphore:
                resp = await client.get(url, timeout=120, follow_redirects=True)
                if resp.status_code == 404:
                    listing_resp = await client.get(STORM_BASE, timeout=30)
                    for line in listing_resp.text.split("\n"):
                        if f"d{year}" in line and "details" in line and ".csv.gz" in line:
                            actual_fn = line.split('"')[1] if '"' in line else line.split()[-1]
                            resp = await client.get(f"{STORM_BASE}/{actual_fn}", timeout=120)
                            break
                    else:
                        print(f"  No storm file found for {year}")
                        return None
                resp.raise_for_status()
                data = gzip.decompress(resp.content)
                df = pd.read_csv(BytesIO(data), low_memory=False)
                df["year"] = year
                return df
        except Exception as e:
            print(f"  NOAA storms failed for {year}: {e}")
            return None

    async def run(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)
        all_dfs = []
        async with httpx.AsyncClient() as client:
            for year in tqdm(self.years, desc="NOAA Storm Events"):
                df = await self._fetch_year(client, year)
                if df is not None:
                    all_dfs.append(df)

        if all_dfs:
            combined = pd.concat(all_dfs, ignore_index=True)
            keep_cols = [
                "BEGIN_YEARMONTH", "BEGIN_DAY", "END_YEARMONTH", "END_DAY",
                "STATE", "STATE_FIPS", "CZ_NAME", "CZ_FIPS", "CZ_TYPE",
                "EVENT_TYPE", "BEGIN_LAT", "BEGIN_LON",
                "DAMAGE_PROPERTY", "DAMAGE_CROPS", "DEATHS_DIRECT",
                "INJURIES_DIRECT", "year",
            ]
            existing = [c for c in keep_cols if c in combined.columns]
            combined = combined[existing]
            self.save_parquet(combined, "noaa_storms_all.parquet")
            print(f"NOAA Storms: {len(combined)} events across {len(all_dfs)} years")
            return combined
        print("NOAA Storms: no data retrieved")
        return pd.DataFrame()
