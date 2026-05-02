import asyncio
import httpx
import pandas as pd
from io import StringIO
from tqdm import tqdm

from src.config import STATE_FIPS, RAW_DIR
from src.ingest.base import BaseIngestor

ECHO_BASE = "https://echodata.epa.gov/echo"


class EPAECHOIngestor(BaseIngestor):
    name = "epa_echo"

    def __init__(self):
        super().__init__()
        self.output_dir = RAW_DIR / "epa_echo"

    async def _fetch_state(self, client: httpx.AsyncClient, state_abbr: str) -> pd.DataFrame | None:
        url = f"{ECHO_BASE}/echo_rest_services.get_facilities"
        params = {
            "p_st": state_abbr,
            "p_act": "Y",
            "output": "CSV",
            "responseset": "5000",
        }
        try:
            async with self._semaphore:
                resp = await client.get(url, params=params, timeout=120)
                resp.raise_for_status()
                df = pd.read_csv(StringIO(resp.text))
                return df
        except Exception as e:
            print(f"  ECHO failed for {state_abbr}: {e}")
            return None

    async def run(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)
        all_dfs = []
        async with httpx.AsyncClient() as client:
            for abbr in tqdm(STATE_FIPS.values(), desc="ECHO Facilities"):
                df = await self._fetch_state(client, abbr)
                if df is not None and not df.empty:
                    all_dfs.append(df)

        if all_dfs:
            combined = pd.concat(all_dfs, ignore_index=True)
            self.save_parquet(combined, "echo_facilities_all.parquet")
            print(f"ECHO: {len(combined)} active facility records")
            return combined
        print("ECHO: no data retrieved")
        return pd.DataFrame()
