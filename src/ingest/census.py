import asyncio
import httpx
import pandas as pd
from tqdm import tqdm

from src.config import CENSUS_API_KEY, CENSUS_VARS, STATE_FIPS, RAW_DIR
from src.ingest.base import BaseIngestor

CENSUS_BASE = "https://api.census.gov/data/2022/acs/acs5"


class CensusIngestor(BaseIngestor):
    name = "census"

    def __init__(self):
        super().__init__()
        self.output_dir = RAW_DIR / "census"

    def _build_url(self, state_fips: str) -> tuple[str, dict]:
        var_list = ",".join(CENSUS_VARS.keys())
        params = {
            "get": var_list,
            "for": "tract:*",
            "in": f"state:{state_fips}",
        }
        if CENSUS_API_KEY:
            params["key"] = CENSUS_API_KEY
        return CENSUS_BASE, params

    def _parse_response(self, data: list, state_fips: str) -> pd.DataFrame:
        header = data[0]
        rows = data[1:]
        df = pd.DataFrame(rows, columns=header)
        rename = {api_var: friendly for api_var, friendly in CENSUS_VARS.items()}
        df = df.rename(columns=rename)
        df["tract_fips"] = df["state"] + df["county"] + df["tract"]
        df["state_fips"] = state_fips
        df["state_abbr"] = STATE_FIPS.get(state_fips, "")
        numeric_cols = list(CENSUS_VARS.values())
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.drop(columns=["state", "county", "tract"], errors="ignore")
        return df

    def fetch_state(self, state_fips: str) -> pd.DataFrame:
        import httpx as httpx_sync
        url, params = self._build_url(state_fips)
        resp = httpx_sync.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return self._parse_response(resp.json(), state_fips)

    def fetch_and_save_state(self, state_fips: str):
        df = self.fetch_state(state_fips)
        self.save_parquet(df, f"census_{state_fips}.parquet")
        return df

    async def _fetch_state_async(self, client: httpx.AsyncClient, state_fips: str) -> pd.DataFrame | None:
        url, params = self._build_url(state_fips)
        try:
            data = await self._fetch_json(client, url, params)
            return self._parse_response(data, state_fips)
        except Exception as e:
            print(f"  FAILED state {state_fips}: {e}")
            return None

    async def run(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)
        all_dfs = []
        async with httpx.AsyncClient() as client:
            tasks = []
            for fips in STATE_FIPS:
                tasks.append(self._fetch_state_async(client, fips))
            results = []
            for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Census ACS"):
                result = await coro
                if result is not None:
                    results.append(result)
        for df in results:
            state = df["state_fips"].iloc[0]
            self.save_parquet(df, f"census_{state}.parquet")
            all_dfs.append(df)
        combined = pd.concat(all_dfs, ignore_index=True)
        self.save_parquet(combined, "census_all.parquet")
        print(f"Census: {len(combined)} tracts across {len(all_dfs)} states")
        return combined
