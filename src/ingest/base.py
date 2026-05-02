import asyncio
import httpx
from pathlib import Path
from abc import ABC, abstractmethod

import pandas as pd
from tqdm import tqdm

from src.config import MAX_CONCURRENT_REQUESTS, REQUEST_TIMEOUT, RETRY_ATTEMPTS, RETRY_BACKOFF


class BaseIngestor(ABC):
    name: str = "base"

    def __init__(self):
        self.output_dir = None
        self._semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async def _fetch_json(self, client: httpx.AsyncClient, url: str, params: dict = None) -> dict | list:
        for attempt in range(RETRY_ATTEMPTS):
            try:
                async with self._semaphore:
                    resp = await client.get(url, params=params, timeout=REQUEST_TIMEOUT)
                    resp.raise_for_status()
                    return resp.json()
            except (httpx.HTTPStatusError, httpx.ReadTimeout, httpx.ConnectTimeout) as e:
                if attempt == RETRY_ATTEMPTS - 1:
                    raise
                wait = RETRY_BACKOFF ** (attempt + 1)
                print(f"  Retry {attempt+1}/{RETRY_ATTEMPTS} for {url}: {e}. Waiting {wait}s")
                await asyncio.sleep(wait)

    def save_parquet(self, df: pd.DataFrame, filename: str):
        self.output_dir.mkdir(parents=True, exist_ok=True)
        path = self.output_dir / filename
        df.to_parquet(path, index=False, engine="pyarrow")
        return path

    @abstractmethod
    async def run(self):
        ...
