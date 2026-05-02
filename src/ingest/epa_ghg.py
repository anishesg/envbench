import asyncio
import httpx
import pandas as pd
from tqdm import tqdm

from src.config import RAW_DIR
from src.ingest.base import BaseIngestor

EPA_BASE = "https://data.epa.gov/efservice"
BATCH_SIZE = 10000


class EPAGHGIngestor(BaseIngestor):
    name = "epa_ghg"

    def __init__(self):
        super().__init__()
        self.output_dir = RAW_DIR / "epa_ghg"

    async def run(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)
        async with httpx.AsyncClient() as client:
            fac_rows = []
            start = 1
            pbar = tqdm(desc="GHG Facilities")
            while True:
                url = f"{EPA_BASE}/ghg_emitter_facilities/year/2022/rows/{start}:{start + BATCH_SIZE - 1}/json"
                try:
                    batch = await self._fetch_json(client, url)
                except Exception:
                    break
                if not batch:
                    break
                fac_rows.extend(batch)
                pbar.update(len(batch))
                if len(batch) < BATCH_SIZE:
                    break
                start += BATCH_SIZE
            pbar.close()

            df = pd.DataFrame(fac_rows) if fac_rows else pd.DataFrame()
            if not df.empty:
                for col in ("latitude", "longitude", "co2e_emission"):
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                self.save_parquet(df, "ghg_facilities_2022.parquet")

            gas_rows = []
            start = 1
            pbar = tqdm(desc="GHG Emissions by Gas")
            while True:
                url = f"{EPA_BASE}/ghg_emitter_gas/year/2022/rows/{start}:{start + BATCH_SIZE - 1}/json"
                try:
                    batch = await self._fetch_json(client, url)
                except Exception:
                    break
                if not batch:
                    break
                gas_rows.extend(batch)
                pbar.update(len(batch))
                if len(batch) < BATCH_SIZE:
                    break
                start += BATCH_SIZE
            pbar.close()

            gas_df = pd.DataFrame(gas_rows) if gas_rows else pd.DataFrame()
            if not gas_df.empty:
                self.save_parquet(gas_df, "ghg_emissions_by_gas_2022.parquet")

        print(f"GHG: {len(df)} facilities, {len(gas_df)} gas emission records")
        return df, gas_df
