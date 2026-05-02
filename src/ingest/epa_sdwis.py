import asyncio
import httpx
import pandas as pd
from tqdm import tqdm

from src.config import STATE_FIPS, RAW_DIR
from src.ingest.base import BaseIngestor

EPA_BASE = "https://data.epa.gov/efservice"
BATCH_SIZE = 10000


class EPASDWISIngestor(BaseIngestor):
    name = "epa_sdwis"

    def __init__(self):
        super().__init__()
        self.output_dir = RAW_DIR / "epa_sdwis"

    async def run(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)
        async with httpx.AsyncClient() as client:
            all_rows = []
            for abbr in tqdm(STATE_FIPS.values(), desc="SDWIS Violations"):
                start = 1
                while True:
                    url = f"{EPA_BASE}/sdwis_violation/primacy_agency_code/{abbr}/rows/{start}:{start + BATCH_SIZE - 1}/json"
                    try:
                        batch = await self._fetch_json(client, url)
                    except Exception:
                        break
                    if not batch:
                        break
                    all_rows.extend(batch)
                    if len(batch) < BATCH_SIZE:
                        break
                    start += BATCH_SIZE

            df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()
            if not df.empty:
                self.save_parquet(df, "sdwis_violations_all.parquet")

            sys_rows = []
            for abbr in tqdm(STATE_FIPS.values(), desc="SDWIS Systems"):
                start = 1
                while True:
                    url = f"{EPA_BASE}/sdwis_water_system/primacy_agency_code/{abbr}/rows/{start}:{start + BATCH_SIZE - 1}/json"
                    try:
                        batch = await self._fetch_json(client, url)
                    except Exception:
                        break
                    if not batch:
                        break
                    sys_rows.extend(batch)
                    if len(batch) < BATCH_SIZE:
                        break
                    start += BATCH_SIZE

            sys_df = pd.DataFrame(sys_rows) if sys_rows else pd.DataFrame()
            if not sys_df.empty:
                self.save_parquet(sys_df, "sdwis_systems_all.parquet")

        print(f"SDWIS: {len(df)} violations, {len(sys_df)} water systems")
        return df, sys_df
