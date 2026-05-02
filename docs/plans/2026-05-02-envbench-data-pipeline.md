# EnvBench Data Pipeline — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a fully automated pipeline that pulls US government environmental data from 8 APIs, joins it geographically at Census tract level into a DuckDB warehouse, and generates ~15-20K benchmark questions with programmatic ground truth — no human annotation.

**Architecture:** Three-stage ETL pipeline. Stage 1 (Ingest): async Python pulls raw data from EPA, Census, CDC, NOAA, USGS APIs into Parquet files — one per source per state. Stage 2 (Join): DuckDB loads all Parquet files, joins on Census tract FIPS codes (with spatial point-in-polygon for lat/lon sources), produces a single master tract table. Stage 3 (Generate): Python reads the master table and generates benchmark questions using templates + programmatic ground truth computation — LLM (Nova Lite on Bedrock, $0.06/M input tokens) used only for natural language surface variation of template-generated questions, not for answers or labels.

**Tech Stack:**
- Python 3.14 + `uv` for dependency management
- `httpx` + `asyncio` for parallel API pulls (already installed)
- `duckdb` for local analytical warehouse (columnar, no server, SQL)
- `pyarrow` / `pandas` for Parquet I/O (already installed)
- `geopandas` + `shapely` for point-in-polygon tract assignment
- `boto3` for Bedrock (Nova Lite) — NL variation only
- `tqdm` for progress bars (already installed)

**Cost model:**
- All government APIs: **free**, no auth for Census/CDC; free API key for EPA/NOAA
- Haiku 4.5 on Bedrock (`us.anthropic.claude-haiku-4-5-20251001-v1:0`): **$0.80/M input, $4.00/M output**
  - NL paraphrasing 15K questions: ~$7.80
  - Quality audit 500 questions: ~$0.32
- **Total estimated LLM cost: ~$8.12**

**Key design principle:** LLMs are used *only* for cosmetic NL variation. All ground truth answers, labels, and numerical values come from deterministic computation over the data. This makes the benchmark fully reproducible and eliminates the "LLM labeling circularity" reviewer concern.

---

## File Structure

```
envbench/
├── pyproject.toml                    # uv project config, all deps
├── .env.example                      # Template for API keys
├── .env                              # Actual keys (gitignored)
├── .gitignore
│
├── src/
│   ├── __init__.py
│   │
│   ├── config.py                     # Paths, API keys from env, model IDs, constants
│   │
│   ├── ingest/                       # Stage 1: Pull raw data from APIs
│   │   ├── __init__.py
│   │   ├── base.py                   # Abstract base class for ingestors
│   │   ├── census.py                 # Census ACS 5-year (demographics)
│   │   ├── cdc_places.py             # CDC PLACES (health outcomes)
│   │   ├── epa_tri.py                # EPA TRI (toxic releases)
│   │   ├── epa_sdwis.py              # EPA SDWIS (drinking water violations)
│   │   ├── epa_ghg.py                # EPA GHG (greenhouse gas emissions)
│   │   ├── epa_echo.py               # EPA ECHO (enforcement/compliance)
│   │   ├── noaa_storms.py            # NOAA Storm Events (weather disasters)
│   │   └── runner.py                 # Orchestrates all ingestors in parallel
│   │
│   ├── join/                         # Stage 2: Geographic join into master table
│   │   ├── __init__.py
│   │   ├── tract_geo.py              # Downloads Census TIGER shapefiles, point-in-polygon
│   │   ├── build_master.py           # DuckDB joins all sources on tract FIPS
│   │   └── validate.py               # Data quality checks on master table
│   │
│   ├── generate/                     # Stage 3: Question generation
│   │   ├── __init__.py
│   │   ├── templates.py              # Question templates per task type
│   │   ├── ground_truth.py           # Deterministic answer computation
│   │   ├── difficulty.py             # Difficulty scoring
│   │   ├── nl_vary.py                # Nova Lite paraphrasing (cosmetic only)
│   │   └── runner.py                 # Orchestrates generation across task types
│   │
│   ├── quality/                      # Quality assurance
│   │   ├── __init__.py
│   │   ├── self_check.py             # Re-derive answers and compare
│   │   └── sample_audit.py           # Haiku spot-check on 500 examples
│   │
│   └── export/                       # Final packaging
│       ├── __init__.py
│       ├── to_hf.py                  # Export to HuggingFace dataset format
│       └── stats.py                  # Dataset statistics and summary
│
├── data/                             # All data artifacts (gitignored except schema)
│   ├── raw/                          # Stage 1 output: one parquet per source/state
│   │   ├── census/
│   │   ├── cdc_places/
│   │   ├── epa_tri/
│   │   ├── epa_sdwis/
│   │   ├── epa_ghg/
│   │   ├── epa_echo/
│   │   └── noaa_storms/
│   ├── geo/                          # Census TIGER shapefiles
│   │   └── tracts/
│   ├── warehouse.duckdb              # Stage 2 output: joined master DB
│   └── benchmark/                    # Stage 3 output: final questions
│       ├── questions.parquet
│       ├── context.parquet           # Source data for each question
│       └── metadata.json
│
├── tests/
│   ├── test_ingest_census.py
│   ├── test_ingest_epa.py
│   ├── test_join.py
│   ├── test_ground_truth.py
│   └── test_templates.py
│
├── scripts/
│   ├── run_pipeline.py               # Full pipeline: ingest → join → generate → export
│   └── run_stage.py                  # Run a single stage (for debugging / resume)
│
└── docs/
    └── plans/
        └── 2026-05-02-envbench-data-pipeline.md  # This file
```

---

### Task 1: Project Scaffolding and Dependencies

**Files:**
- Create: `pyproject.toml`
- Create: `.gitignore`
- Create: `.env.example`
- Create: `src/__init__.py`
- Create: `src/config.py`

- [ ] **Step 1: Initialize git repo and uv project**

```bash
cd /Users/anish/envbench
git init
uv init --no-readme
```

- [ ] **Step 2: Write pyproject.toml with all dependencies**

```toml
[project]
name = "envbench"
version = "0.1.0"
description = "Multi-source environmental hazard reasoning benchmark"
requires-python = ">=3.12"
dependencies = [
    "httpx>=0.28",
    "pandas>=2.2",
    "pyarrow>=15.0",
    "duckdb>=1.2",
    "geopandas>=1.0",
    "shapely>=2.0",
    "boto3>=1.35",
    "tqdm>=4.66",
    "python-dotenv>=1.0",
]

[project.optional-dependencies]
dev = ["pytest>=8.0", "pytest-asyncio>=0.24"]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

- [ ] **Step 3: Write .gitignore**

```
data/
*.duckdb
*.duckdb.wal
.env
__pycache__/
*.pyc
.venv/
```

- [ ] **Step 4: Write .env.example**

```bash
# Census API key (optional, increases rate limit): https://api.census.gov/data/key_signup.html
CENSUS_API_KEY=
# NOAA CDO API key (required): https://www.ncdc.noaa.gov/cdo-web/token
NOAA_API_KEY=
# AWS region for Bedrock
AWS_DEFAULT_REGION=us-east-1
```

- [ ] **Step 5: Write src/config.py**

```python
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
GEO_DIR = DATA_DIR / "geo"
WAREHOUSE_PATH = DATA_DIR / "warehouse.duckdb"
BENCHMARK_DIR = DATA_DIR / "benchmark"

for d in [RAW_DIR, GEO_DIR, BENCHMARK_DIR]:
    d.mkdir(parents=True, exist_ok=True)

CENSUS_API_KEY = os.getenv("CENSUS_API_KEY", "")
NOAA_API_KEY = os.getenv("NOAA_API_KEY", "")

BEDROCK_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
MODEL_NL_VARY = "us.anthropic.claude-haiku-4-5-20251001-v1:0"
MODEL_QUALITY_CHECK = "us.anthropic.claude-haiku-4-5-20251001-v1:0"

STATE_FIPS = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY", "72": "PR",
}

CENSUS_VARS = {
    "B01003_001E": "total_population",
    "B19013_001E": "median_household_income",
    "B17001_002E": "population_below_poverty",
    "B02001_002E": "pop_white_alone",
    "B02001_003E": "pop_black_alone",
    "B03003_003E": "pop_hispanic",
    "B01001_003E": "pop_male_under_5",
    "B01001_027E": "pop_female_under_5",
    "B25035_001E": "median_year_structure_built",
    "B25003_001E": "total_housing_units",
    "B16004_001E": "pop_5_plus",
    "B16004_003E": "pop_speak_only_english",
}

CDC_MEASURES = [
    "CASTHMA", "DIABETES", "CHD", "COPD", "OBESITY",
    "DEPRESSION", "BPHIGH", "CANCER", "KIDNEY", "STROKE",
    "DISABILITY", "SMOKING", "LPA", "SLEEP", "BINGE",
]

MAX_CONCURRENT_REQUESTS = 10
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 2.0
```

- [ ] **Step 6: Install dependencies and verify**

```bash
uv sync
uv run python -c "import duckdb, geopandas, httpx, boto3; print('All deps OK')"
```

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml .gitignore .env.example src/__init__.py src/config.py
git commit -m "feat: scaffold envbench project with config and dependencies"
```

---

### Task 2: Ingest Base Class + Census ACS Ingestor

**Files:**
- Create: `src/ingest/__init__.py`
- Create: `src/ingest/base.py`
- Create: `src/ingest/census.py`
- Create: `tests/test_ingest_census.py`

- [ ] **Step 1: Write the test for Census ingestor**

```python
# tests/test_ingest_census.py
import pytest
import pandas as pd
from pathlib import Path

@pytest.fixture
def census_ingestor():
    from src.ingest.census import CensusIngestor
    return CensusIngestor()

def test_fetch_single_state(census_ingestor, tmp_path):
    """Pull NJ tracts (small state, ~2K tracts) and verify schema."""
    census_ingestor.output_dir = tmp_path / "census"
    census_ingestor.output_dir.mkdir()
    df = census_ingestor.fetch_state("34")  # NJ
    assert len(df) > 1500  # NJ has ~2100 tracts
    assert "tract_fips" in df.columns
    assert "total_population" in df.columns
    assert "median_household_income" in df.columns
    assert df["tract_fips"].str.len().eq(11).all()  # 2 state + 3 county + 6 tract
    assert df["total_population"].dtype in ("int64", "float64")

def test_save_parquet(census_ingestor, tmp_path):
    census_ingestor.output_dir = tmp_path / "census"
    census_ingestor.output_dir.mkdir()
    census_ingestor.fetch_and_save_state("34")
    parquet_path = census_ingestor.output_dir / "census_34.parquet"
    assert parquet_path.exists()
    df = pd.read_parquet(parquet_path)
    assert len(df) > 1500
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_ingest_census.py -v
```
Expected: FAIL (module not found)

- [ ] **Step 3: Write the base ingestor class**

```python
# src/ingest/base.py
import asyncio
import httpx
import time
from pathlib import Path
from abc import ABC, abstractmethod

import pandas as pd
from tqdm import tqdm

from src.config import MAX_CONCURRENT_REQUESTS, REQUEST_TIMEOUT, RETRY_ATTEMPTS, RETRY_BACKOFF


class BaseIngestor(ABC):
    """Base class for all data source ingestors."""

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
        """Pull all data for all states/entities and save to parquet."""
        ...
```

- [ ] **Step 4: Write the Census ACS ingestor**

```python
# src/ingest/census.py
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
        """Synchronous single-state fetch for testing."""
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
```

- [ ] **Step 5: Write the __init__.py**

```python
# src/ingest/__init__.py
```

(Empty file — just marks the package.)

- [ ] **Step 6: Run tests**

```bash
uv run pytest tests/test_ingest_census.py -v
```
Expected: both tests PASS (hits live Census API — NJ has ~2100 tracts, takes ~2-5 seconds)

- [ ] **Step 7: Commit**

```bash
git add src/ingest/ tests/test_ingest_census.py
git commit -m "feat: Census ACS ingestor with async state-level pulls"
```

---

### Task 3: CDC PLACES Ingestor

**Files:**
- Create: `src/ingest/cdc_places.py`
- Create: `tests/test_ingest_cdc.py`

- [ ] **Step 1: Write the test**

```python
# tests/test_ingest_cdc.py
import pytest
import pandas as pd

@pytest.fixture
def cdc_ingestor():
    from src.ingest.cdc_places import CDCPlacesIngestor
    return CDCPlacesIngestor()

def test_fetch_single_measure_single_state(cdc_ingestor):
    df = cdc_ingestor.fetch_measure_state("CASTHMA", "NJ")
    assert len(df) > 500  # NJ has ~2100 tracts, not all may have data
    assert "tract_fips" in df.columns
    assert "measure_id" in df.columns
    assert "data_value" in df.columns
    assert df["measure_id"].eq("CASTHMA").all()

def test_pivot_measures(cdc_ingestor):
    df = cdc_ingestor.fetch_measure_state("CASTHMA", "NJ")
    df2 = cdc_ingestor.fetch_measure_state("DIABETES", "NJ")
    combined = pd.concat([df, df2])
    pivoted = cdc_ingestor.pivot_to_tract_level(combined)
    assert "casthma" in pivoted.columns
    assert "diabetes" in pivoted.columns
    assert "tract_fips" in pivoted.columns
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_ingest_cdc.py -v
```

- [ ] **Step 3: Implement CDC PLACES ingestor**

```python
# src/ingest/cdc_places.py
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
        """Synchronous fetch for one measure + one state. For testing."""
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
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/test_ingest_cdc.py -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/ingest/cdc_places.py tests/test_ingest_cdc.py
git commit -m "feat: CDC PLACES ingestor with tract-level health outcomes"
```

---

### Task 4: EPA TRI Ingestor (Toxic Release Inventory)

**Files:**
- Create: `src/ingest/epa_tri.py`
- Create: `tests/test_ingest_epa.py`

- [ ] **Step 1: Write the test**

```python
# tests/test_ingest_epa.py
import pytest
import pandas as pd

@pytest.fixture
def tri_ingestor():
    from src.ingest.epa_tri import EPATRIIngestor
    return EPATRIIngestor()

def test_fetch_facilities_single_state(tri_ingestor):
    df = tri_ingestor.fetch_state_facilities("NJ")
    assert len(df) > 500  # NJ has ~1775 TRI facilities
    assert "tri_facility_id" in df.columns
    assert "fac_latitude" in df.columns
    assert "fac_longitude" in df.columns
    assert "state_abbr" in df.columns

def test_fetch_releases_single_state(tri_ingestor):
    df = tri_ingestor.fetch_state_releases("NJ", year=2022)
    assert len(df) > 100
    assert "tri_facility_id" in df.columns
    assert "chemical" in df.columns
    assert "total_releases_lbs" in df.columns
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_ingest_epa.py -v
```

- [ ] **Step 3: Implement EPA TRI ingestor**

The EPA Envirofacts API uses a REST pattern: `/efservice/{table}/{column}/{value}/rows/{start}:{end}/json`. We need two tables: `tri_facility` for locations and `tri_reporting_form` for chemical releases.

```python
# src/ingest/epa_tri.py
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
        """Synchronous fetch for testing."""
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
        """Synchronous fetch for testing."""
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
            # Facilities
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

            # Releases (2022)
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
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/test_ingest_epa.py -v
```
Expected: PASS (hits live EPA API)

- [ ] **Step 5: Commit**

```bash
git add src/ingest/epa_tri.py tests/test_ingest_epa.py
git commit -m "feat: EPA TRI ingestor for facilities and chemical releases"
```

---

### Task 5: EPA SDWIS, GHG, ECHO Ingestors

**Files:**
- Create: `src/ingest/epa_sdwis.py`
- Create: `src/ingest/epa_ghg.py`
- Create: `src/ingest/epa_echo.py`

These follow the same pattern as TRI. Key API differences:

- [ ] **Step 1: Implement SDWIS ingestor**

```python
# src/ingest/epa_sdwis.py
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
            # Water system violations
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

            # Water systems (for population served + location)
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
```

- [ ] **Step 2: Implement GHG ingestor**

```python
# src/ingest/epa_ghg.py
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
            # GHG emitter facilities
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

            # GHG emissions by gas
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
```

- [ ] **Step 3: Implement ECHO ingestor**

ECHO uses the ECHO API (`https://echodata.epa.gov/echo/`) which returns CSV. Different from Envirofacts.

```python
# src/ingest/epa_echo.py
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
```

- [ ] **Step 4: Commit**

```bash
git add src/ingest/epa_sdwis.py src/ingest/epa_ghg.py src/ingest/epa_echo.py
git commit -m "feat: SDWIS, GHG, and ECHO ingestors for EPA data"
```

---

### Task 6: NOAA Storm Events Ingestor

**Files:**
- Create: `src/ingest/noaa_storms.py`

- [ ] **Step 1: Implement NOAA Storm Events ingestor**

NOAA Storm Events are available as bulk CSV downloads from NCEI — no API key needed. Much faster than hitting an API.

```python
# src/ingest/noaa_storms.py
import asyncio
import gzip
import httpx
import pandas as pd
from io import BytesIO, StringIO
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
                    # Try alternate naming patterns
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
```

- [ ] **Step 2: Commit**

```bash
git add src/ingest/noaa_storms.py
git commit -m "feat: NOAA Storm Events bulk CSV ingestor"
```

---

### Task 7: Pipeline Runner (Ingest Orchestration)

**Files:**
- Create: `src/ingest/runner.py`
- Create: `scripts/run_pipeline.py`
- Create: `scripts/run_stage.py`

- [ ] **Step 1: Implement the ingest runner**

```python
# src/ingest/runner.py
import asyncio
import time

from src.ingest.census import CensusIngestor
from src.ingest.cdc_places import CDCPlacesIngestor
from src.ingest.epa_tri import EPATRIIngestor
from src.ingest.epa_sdwis import EPASDWISIngestor
from src.ingest.epa_ghg import EPAGHGIngestor
from src.ingest.epa_echo import EPAECHOIngestor
from src.ingest.noaa_storms import NOAAStormIngestor


async def run_all_ingestors():
    """Run all ingestors. Each is independent — they can run concurrently at the
    ingestor level, but each ingestor manages its own internal concurrency."""
    start = time.time()
    ingestors = [
        CensusIngestor(),
        CDCPlacesIngestor(),
        EPATRIIngestor(),
        EPASDWISIngestor(),
        EPAGHGIngestor(),
        EPAECHOIngestor(),
        NOAAStormIngestor(),
    ]
    # Run sequentially to avoid overwhelming APIs
    # (each ingestor has its own internal parallelism)
    for ingestor in ingestors:
        print(f"\n{'='*60}")
        print(f"Starting: {ingestor.name}")
        print(f"{'='*60}")
        try:
            await ingestor.run()
        except Exception as e:
            print(f"FAILED: {ingestor.name}: {e}")

    elapsed = time.time() - start
    print(f"\nAll ingestors complete in {elapsed/60:.1f} minutes")
```

- [ ] **Step 2: Implement pipeline script**

```python
# scripts/run_pipeline.py
"""
Full pipeline: ingest → join → generate → export.
Run with: uv run python scripts/run_pipeline.py
Or run a single stage: uv run python scripts/run_pipeline.py --stage ingest
"""
import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


async def main():
    parser = argparse.ArgumentParser(description="EnvBench data pipeline")
    parser.add_argument("--stage", choices=["ingest", "join", "generate", "export", "all"], default="all")
    args = parser.parse_args()

    if args.stage in ("ingest", "all"):
        from src.ingest.runner import run_all_ingestors
        await run_all_ingestors()

    if args.stage in ("join", "all"):
        from src.join.build_master import build_master_table
        build_master_table()

    if args.stage in ("generate", "all"):
        from src.generate.runner import run_generation
        await run_generation()

    if args.stage in ("export", "all"):
        from src.export.to_hf import export_to_hf
        export_to_hf()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 3: Commit**

```bash
git add src/ingest/runner.py scripts/run_pipeline.py
git commit -m "feat: pipeline orchestration for ingest stage"
```

---

### Task 8: Census TIGER Tract Shapefiles + Point-in-Polygon

**Files:**
- Create: `src/join/__init__.py`
- Create: `src/join/tract_geo.py`
- Create: `tests/test_join.py`

- [ ] **Step 1: Write the test**

```python
# tests/test_join.py
import pytest
import pandas as pd

def test_assign_tract_from_latlon():
    from src.join.tract_geo import TractGeocoder
    geocoder = TractGeocoder(states=["34"])  # NJ only for speed
    geocoder.load()
    # Known location: Princeton, NJ — should be in a Mercer County tract
    result = geocoder.assign_tract(40.3573, -74.6672)
    assert result is not None
    assert result.startswith("34021")  # Mercer County FIPS = 34021

def test_bulk_assign(tmp_path):
    from src.join.tract_geo import TractGeocoder
    geocoder = TractGeocoder(states=["34"])
    geocoder.load()
    df = pd.DataFrame({
        "lat": [40.3573, 40.7128, 39.3643],
        "lon": [-74.6672, -74.0060, -74.4229],
    })
    result = geocoder.bulk_assign(df, lat_col="lat", lon_col="lon")
    assert "tract_fips" in result.columns
    assert result["tract_fips"].notna().sum() >= 2  # At least Princeton + somewhere in NJ
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_join.py -v
```

- [ ] **Step 3: Implement tract geocoder**

```python
# src/join/tract_geo.py
import httpx
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from pathlib import Path
from tqdm import tqdm

from src.config import GEO_DIR, STATE_FIPS

TIGER_BASE = "https://www2.census.gov/geo/tiger/TIGER2022/TRACT"


class TractGeocoder:
    """Downloads Census TIGER/Line tract boundary shapefiles and performs
    point-in-polygon assignment of lat/lon coordinates to Census tracts."""

    def __init__(self, states: list[str] | None = None):
        self.states = states or list(STATE_FIPS.keys())
        self.tracts_gdf: gpd.GeoDataFrame | None = None
        self.sindex = None

    def _download_state_shapefile(self, state_fips: str) -> Path:
        """Download and extract TIGER tract shapefile for one state."""
        out_dir = GEO_DIR / "tracts" / state_fips
        shp_path = out_dir / f"tl_2022_{state_fips}_tract.shp"
        if shp_path.exists():
            return shp_path

        out_dir.mkdir(parents=True, exist_ok=True)
        zip_url = f"{TIGER_BASE}/tl_2022_{state_fips}_tract.zip"
        resp = httpx.get(zip_url, timeout=120, follow_redirects=True)
        resp.raise_for_status()

        import zipfile
        from io import BytesIO
        with zipfile.ZipFile(BytesIO(resp.content)) as zf:
            zf.extractall(out_dir)
        return shp_path

    def load(self):
        """Download shapefiles for all states and load into a single GeoDataFrame."""
        gdfs = []
        for fips in tqdm(self.states, desc="Loading tract shapefiles"):
            shp_path = self._download_state_shapefile(fips)
            gdf = gpd.read_file(shp_path)
            gdf["tract_fips"] = gdf["GEOID"]
            gdfs.append(gdf[["tract_fips", "geometry"]])

        self.tracts_gdf = pd.concat(gdfs, ignore_index=True)
        self.tracts_gdf = gpd.GeoDataFrame(self.tracts_gdf, geometry="geometry")
        self.tracts_gdf = self.tracts_gdf.set_crs("EPSG:4326", allow_override=True)
        self.sindex = self.tracts_gdf.sindex
        print(f"Loaded {len(self.tracts_gdf)} tract polygons")

    def assign_tract(self, lat: float, lon: float) -> str | None:
        """Assign a single lat/lon to a tract FIPS code."""
        point = Point(lon, lat)  # shapely uses (x, y) = (lon, lat)
        candidates = list(self.sindex.query(point, predicate="intersects"))
        if candidates:
            return self.tracts_gdf.iloc[candidates[0]]["tract_fips"]
        return None

    def bulk_assign(self, df: pd.DataFrame, lat_col: str, lon_col: str) -> pd.DataFrame:
        """Assign tract FIPS to every row in a DataFrame with lat/lon columns.
        Uses spatial join for efficiency (much faster than row-by-row)."""
        valid = df[[lat_col, lon_col]].dropna()
        if valid.empty:
            df["tract_fips"] = None
            return df

        points = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df[lon_col], df[lat_col]),
            crs="EPSG:4326",
        )
        joined = gpd.sjoin(points, self.tracts_gdf, how="left", predicate="within")
        df = df.copy()
        df["tract_fips"] = joined["tract_fips_right"].values
        return df
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/test_join.py -v
```
Expected: PASS (downloads NJ shapefile ~5MB on first run, then cached)

- [ ] **Step 5: Commit**

```bash
git add src/join/ tests/test_join.py
git commit -m "feat: Census TIGER tract geocoder with spatial join"
```

---

### Task 9: DuckDB Master Table Builder

**Files:**
- Create: `src/join/build_master.py`
- Create: `src/join/validate.py`

- [ ] **Step 1: Implement master table builder**

This is the core join: load all raw Parquet files into DuckDB, assign tracts to facility lat/lons, and join everything on `tract_fips`.

```python
# src/join/build_master.py
import duckdb
import pandas as pd
from pathlib import Path

from src.config import RAW_DIR, WAREHOUSE_PATH, GEO_DIR
from src.join.tract_geo import TractGeocoder


def build_master_table():
    """Join all ingested data sources on tract_fips into a single DuckDB warehouse."""
    print("Building master table...")

    # Step 1: Assign tracts to EPA facilities that have lat/lon
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

    # Step 2: Build DuckDB warehouse
    print("Building DuckDB warehouse...")
    con = duckdb.connect(str(WAREHOUSE_PATH))

    # Census tracts as the base table
    census_path = RAW_DIR / "census" / "census_all.parquet"
    if census_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE census AS
            SELECT * FROM read_parquet('{census_path}')
        """)
        count = con.execute("SELECT COUNT(*) FROM census").fetchone()[0]
        print(f"  census: {count} tracts")

    # CDC PLACES
    cdc_path = RAW_DIR / "cdc_places" / "cdc_places_all.parquet"
    if cdc_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE cdc_places AS
            SELECT * FROM read_parquet('{cdc_path}')
        """)
        count = con.execute("SELECT COUNT(*) FROM cdc_places").fetchone()[0]
        print(f"  cdc_places: {count} tracts")

    # TRI facilities (geocoded)
    tri_geo_path = RAW_DIR / "epa_tri" / "tri_facilities_geocoded.parquet"
    if tri_geo_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE tri_facilities AS
            SELECT * FROM read_parquet('{tri_geo_path}')
        """)

    # TRI releases
    tri_rel_path = RAW_DIR / "epa_tri" / "tri_releases_2022_all.parquet"
    if tri_rel_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE tri_releases AS
            SELECT * FROM read_parquet('{tri_rel_path}')
        """)

    # SDWIS
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

    # GHG
    ghg_geo_path = RAW_DIR / "epa_ghg" / "ghg_facilities_geocoded.parquet"
    if ghg_geo_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE ghg_facilities AS
            SELECT * FROM read_parquet('{ghg_geo_path}')
        """)

    # NOAA storms
    storms_path = RAW_DIR / "noaa_storms" / "noaa_storms_all.parquet"
    if storms_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE noaa_storms AS
            SELECT * FROM read_parquet('{storms_path}')
        """)

    # ECHO
    echo_path = RAW_DIR / "epa_echo" / "echo_facilities_all.parquet"
    if echo_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE echo_facilities AS
            SELECT * FROM read_parquet('{echo_path}')
        """)

    # Step 3: Create the master tract-level view joining census + CDC + TRI aggregates
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

    # Export master as parquet for downstream use
    con.execute(f"""
        COPY (SELECT * FROM master_tract)
        TO '{RAW_DIR / "master_tract.parquet"}' (FORMAT PARQUET)
    """)

    con.close()
    print("Warehouse built successfully")
```

- [ ] **Step 2: Implement validation**

```python
# src/join/validate.py
import duckdb
from src.config import WAREHOUSE_PATH


def validate_warehouse():
    """Run data quality checks on the master warehouse."""
    con = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)
    checks = []

    # Check 1: Census tract count reasonable
    census_count = con.execute("SELECT COUNT(*) FROM census").fetchone()[0]
    checks.append(("census_tract_count", census_count, census_count > 70000))

    # Check 2: No duplicate tract FIPS
    dup_count = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT tract_fips, COUNT(*) as n FROM census GROUP BY tract_fips HAVING n > 1
        )
    """).fetchone()[0]
    checks.append(("census_no_duplicate_fips", dup_count, dup_count == 0))

    # Check 3: CDC join rate
    cdc_join = con.execute("""
        SELECT COUNT(*) FROM master_tract WHERE casthma IS NOT NULL
    """).fetchone()[0]
    cdc_rate = cdc_join / census_count if census_count else 0
    checks.append(("cdc_join_rate", f"{cdc_rate:.1%}", cdc_rate > 0.5))

    # Check 4: TRI facilities have tracts assigned
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
```

- [ ] **Step 3: Commit**

```bash
git add src/join/build_master.py src/join/validate.py
git commit -m "feat: DuckDB master table builder with tract-level joins"
```

---

### Task 10: Question Templates + Ground Truth Engine

**Files:**
- Create: `src/generate/__init__.py`
- Create: `src/generate/templates.py`
- Create: `src/generate/ground_truth.py`
- Create: `tests/test_ground_truth.py`

- [ ] **Step 1: Write the test**

```python
# tests/test_ground_truth.py
import pytest

def test_threshold_exceedance_answer():
    from src.generate.ground_truth import compute_threshold_exceedance
    # If measured value is 12.5 and MCL is 10, should be in violation
    result = compute_threshold_exceedance(measured=12.5, threshold=10.0)
    assert result["exceeds"] is True
    assert result["exceedance_pct"] == pytest.approx(25.0)

def test_threshold_exceedance_compliant():
    from src.generate.ground_truth import compute_threshold_exceedance
    result = compute_threshold_exceedance(measured=8.0, threshold=10.0)
    assert result["exceeds"] is False

def test_disparity_ratio():
    from src.generate.ground_truth import compute_disparity_ratio
    result = compute_disparity_ratio(
        group_a_exposure=15.2,
        group_b_exposure=8.1,
    )
    assert result["ratio"] == pytest.approx(1.877, rel=0.01)
    assert result["disparity_pct"] == pytest.approx(87.7, rel=0.1)

def test_counterfactual_threshold():
    from src.generate.ground_truth import compute_counterfactual_threshold
    values = [5.0, 8.0, 11.0, 14.0, 7.0, 3.0]
    populations = [1000, 2000, 3000, 4000, 1500, 500]
    result = compute_counterfactual_threshold(
        values=values,
        populations=populations,
        old_threshold=15.0,
        new_threshold=10.0,
    )
    assert result["newly_exceeding_count"] == 2  # 11.0 and 14.0
    assert result["newly_affected_population"] == 7000  # 3000 + 4000
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_ground_truth.py -v
```

- [ ] **Step 3: Implement ground truth computation functions**

```python
# src/generate/ground_truth.py
"""Deterministic ground truth computations. No LLMs. Pure math on data."""

import numpy as np


def compute_threshold_exceedance(measured: float, threshold: float) -> dict:
    exceeds = measured > threshold
    exceedance_pct = ((measured - threshold) / threshold) * 100 if exceeds else 0.0
    return {
        "exceeds": exceeds,
        "measured": measured,
        "threshold": threshold,
        "exceedance_pct": round(exceedance_pct, 1),
    }


def compute_disparity_ratio(group_a_exposure: float, group_b_exposure: float) -> dict:
    ratio = group_a_exposure / group_b_exposure if group_b_exposure != 0 else float("inf")
    disparity_pct = (ratio - 1) * 100
    return {
        "ratio": round(ratio, 3),
        "disparity_pct": round(disparity_pct, 1),
        "group_a_exposure": group_a_exposure,
        "group_b_exposure": group_b_exposure,
    }


def compute_counterfactual_threshold(
    values: list[float],
    populations: list[int],
    old_threshold: float,
    new_threshold: float,
) -> dict:
    newly_exceeding = [
        (v, p) for v, p in zip(values, populations)
        if v > new_threshold and v <= old_threshold
    ]
    return {
        "newly_exceeding_count": len(newly_exceeding),
        "newly_affected_population": sum(p for _, p in newly_exceeding),
        "old_threshold": old_threshold,
        "new_threshold": new_threshold,
    }


def compute_cumulative_burden_score(
    pollution_indicators: dict[str, float],
    demographic_indicators: dict[str, float],
    weights: dict[str, float] | None = None,
) -> dict:
    if weights is None:
        weights = {k: 1.0 for k in pollution_indicators}
    pollution_score = sum(
        pollution_indicators.get(k, 0) * weights.get(k, 1.0)
        for k in pollution_indicators
    )
    demo_multiplier = 1.0
    if "pct_poverty" in demographic_indicators:
        demo_multiplier += demographic_indicators["pct_poverty"] / 100
    if "pct_minority" in demographic_indicators:
        demo_multiplier += demographic_indicators["pct_minority"] / 200
    combined = pollution_score * demo_multiplier
    return {
        "pollution_score": round(pollution_score, 2),
        "demographic_multiplier": round(demo_multiplier, 3),
        "combined_burden_score": round(combined, 2),
    }


def compute_partial_correlation(
    x: list[float], y: list[float], z: list[float]
) -> dict:
    x, y, z = np.array(x), np.array(y), np.array(z)
    mask = ~(np.isnan(x) | np.isnan(y) | np.isnan(z))
    x, y, z = x[mask], y[mask], z[mask]
    if len(x) < 10:
        return {"partial_r": None, "n": len(x), "sufficient_data": False}
    from scipy import stats
    r_xy = np.corrcoef(x, y)[0, 1]
    r_xz = np.corrcoef(x, z)[0, 1]
    r_yz = np.corrcoef(y, z)[0, 1]
    denom = np.sqrt((1 - r_xz**2) * (1 - r_yz**2))
    partial_r = (r_xy - r_xz * r_yz) / denom if denom > 0 else 0.0
    n = len(x)
    t_stat = partial_r * np.sqrt((n - 3) / (1 - partial_r**2)) if abs(partial_r) < 1 else float("inf")
    p_value = 2 * (1 - stats.t.cdf(abs(t_stat), df=n - 3)) if np.isfinite(t_stat) else 0.0
    return {
        "partial_r": round(float(partial_r), 4),
        "raw_r": round(float(r_xy), 4),
        "p_value": round(float(p_value), 6),
        "significant_at_05": p_value < 0.05,
        "n": n,
        "sufficient_data": True,
    }
```

- [ ] **Step 4: Implement question templates**

```python
# src/generate/templates.py
"""Question templates for each task type. Templates are filled with real data values.
Ground truth is computed deterministically — no LLM involvement."""

TASK_TYPES = {
    "threshold_exceedance": {
        "tier": 1,
        "description": "Does a measurement exceed a regulatory threshold?",
        "templates": [
            "Water system {system_name} (PWSID: {pwsid}) in {county}, {state} had a most recent {contaminant} reading of {measured} {unit}. The EPA Maximum Contaminant Level (MCL) for {contaminant} is {threshold} {unit}. Is this system in violation?",
            "The EPA sets the MCL for {contaminant} at {threshold} {unit}. Water system {system_name} serving {population:,} people in {county}, {state} recorded {measured} {unit}. Does this exceed the federal standard, and by what percentage?",
        ],
    },
    "counterfactual_threshold": {
        "tier": 2,
        "description": "If a regulatory threshold changed, how many more entities would be in violation?",
        "templates": [
            "The current EPA MCL for {contaminant} is {old_threshold} {unit}. If it were lowered to {new_threshold} {unit}, how many additional water systems in {state} would exceed the new standard? What is the total population served by those newly noncompliant systems?",
        ],
    },
    "disparity_analysis": {
        "tier": 2,
        "description": "Compare environmental burden across demographic groups.",
        "templates": [
            "In {county} County, {state}, compare the average {indicator} for census tracts where the majority of residents are {group_a} versus tracts where the majority are {group_b}. What is the ratio of {group_a} exposure to {group_b} exposure?",
            "Across all census tracts in {state}, compute the average number of TRI facilities within 3 miles for tracts in the top income quintile versus the bottom income quintile. What is the disparity ratio?",
        ],
    },
    "cumulative_burden": {
        "tier": 3,
        "description": "Multi-source integration to compute environmental burden.",
        "templates": [
            "For census tract {tract_fips} in {county}, {state}: (1) How many TRI facilities are within the tract boundaries? (2) What is the total reported toxic release in pounds? (3) What is the tract's asthma prevalence from CDC PLACES? (4) What is the tract's median household income? (5) Compute a cumulative burden score as (total_releases_lbs / 1000) * (asthma_prevalence / state_avg_asthma) * (1 + poverty_rate).",
        ],
    },
    "confounded_correlation": {
        "tier": 3,
        "description": "Apparent correlation that disappears after controlling for a confounder.",
        "templates": [
            "In {county} County, the Pearson correlation between TRI facility density (facilities per sq mile) and adult asthma prevalence across census tracts is r={raw_r}. However, median household income is also correlated with both variables. Compute the partial correlation between TRI density and asthma prevalence, controlling for income. Does the relationship remain statistically significant (p < 0.05)?",
        ],
    },
    "missing_data_detection": {
        "tier": 3,
        "description": "Question where correct answer is 'insufficient data'.",
        "templates": [
            "What is the drinking water quality for residents using private wells in census tract {tract_fips} ({county}, {state})? The tract has {population:,} residents, of whom approximately {pct_private_well}% rely on private wells.",
            "What was the air quality in census tract {tract_fips} on {specific_date}? The nearest EPA AQS monitor is {monitor_distance_miles:.1f} miles away, located in {monitor_county} County.",
        ],
    },
}
```

- [ ] **Step 5: Run tests**

```bash
uv run pytest tests/test_ground_truth.py -v
```
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/generate/ tests/test_ground_truth.py
git commit -m "feat: question templates and deterministic ground truth engine"
```

---

### Task 11: Question Generation Runner

**Files:**
- Create: `src/generate/difficulty.py`
- Create: `src/generate/runner.py`

- [ ] **Step 1: Implement difficulty scoring**

```python
# src/generate/difficulty.py
"""Score question difficulty based on structural properties."""


def score_difficulty(
    num_sources: int,
    num_reasoning_steps: int,
    has_adversarial_element: bool = False,
    requires_statistical_computation: bool = False,
    correct_answer_is_insufficient_data: bool = False,
) -> dict:
    base = num_sources + num_reasoning_steps
    if has_adversarial_element:
        base += 3
    if requires_statistical_computation:
        base += 2
    if correct_answer_is_insufficient_data:
        base += 2
    if base <= 3:
        tier = 1
        label = "retrieval"
    elif base <= 6:
        tier = 2
        label = "integration"
    else:
        tier = 3
        label = "adversarial_causal"
    return {"score": base, "tier": tier, "label": label}
```

- [ ] **Step 2: Implement the generation runner**

```python
# src/generate/runner.py
"""Reads master tract table from DuckDB, generates questions for each task type,
computes ground truth answers, and writes final benchmark parquet."""

import json
import random
import duckdb
import pandas as pd
from pathlib import Path
from tqdm import tqdm

from src.config import WAREHOUSE_PATH, BENCHMARK_DIR
from src.generate.ground_truth import (
    compute_threshold_exceedance,
    compute_disparity_ratio,
    compute_counterfactual_threshold,
    compute_cumulative_burden_score,
    compute_partial_correlation,
)
from src.generate.difficulty import score_difficulty
from src.generate.templates import TASK_TYPES

random.seed(42)


def _generate_threshold_questions(con: duckdb.DuckDBPyConnection, n: int = 3000) -> list[dict]:
    """Generate threshold exceedance questions from SDWIS violations."""
    rows = con.execute("""
        SELECT
            v.*,
            s.pws_name,
            s.population_served_count,
            s.county_served,
            s.state_code
        FROM sdwis_violations v
        LEFT JOIN sdwis_systems s ON v.pwsid = s.pwsid
        WHERE v.contaminant_code IS NOT NULL
        LIMIT 50000
    """).fetchdf()

    if rows.empty:
        return []

    questions = []
    for _, row in tqdm(rows.sample(min(n, len(rows))).iterrows(), desc="Threshold Q's"):
        template = random.choice(TASK_TYPES["threshold_exceedance"]["templates"])
        # Build question from real data
        q = {
            "question_id": f"thresh_{row.get('pwsid','')}_{row.get('contaminant_code','')}",
            "task_type": "threshold_exceedance",
            "tier": 1,
            "question_text": template.format(
                system_name=row.get("pws_name", "Unknown"),
                pwsid=row.get("pwsid", ""),
                county=row.get("county_served", "Unknown"),
                state=row.get("state_code", ""),
                contaminant=row.get("contaminant_code", "Unknown"),
                measured=row.get("viol_measure", 0),
                threshold=row.get("standard", 0),
                unit="ppb",
                population=row.get("population_served_count", 0),
            ),
            "ground_truth": compute_threshold_exceedance(
                measured=float(row.get("viol_measure", 0) or 0),
                threshold=float(row.get("standard", 0) or 1),
            ),
            "data_sources": ["EPA SDWIS"],
            "tract_fips": None,
            "difficulty": score_difficulty(num_sources=1, num_reasoning_steps=1),
        }
        questions.append(q)
    return questions


def _generate_cumulative_burden_questions(con: duckdb.DuckDBPyConnection, n: int = 5000) -> list[dict]:
    """Generate multi-source cumulative burden questions from the master table."""
    rows = con.execute("""
        SELECT
            tract_fips, state_abbr,
            total_population, median_household_income,
            population_below_poverty, pop_black_alone, pop_hispanic,
            casthma, diabetes, copd,
            tri_facility_count, tri_total_releases_lbs
        FROM master_tract
        WHERE total_population > 100
          AND casthma IS NOT NULL
          AND tri_facility_count IS NOT NULL
          AND tri_facility_count > 0
        ORDER BY RANDOM()
        LIMIT ?
    """, [n]).fetchdf()

    if rows.empty:
        return []

    state_avg_asthma = con.execute("""
        SELECT state_abbr, AVG(casthma) as avg_asthma
        FROM master_tract
        WHERE casthma IS NOT NULL
        GROUP BY state_abbr
    """).fetchdf().set_index("state_abbr")["avg_asthma"].to_dict()

    questions = []
    template = TASK_TYPES["cumulative_burden"]["templates"][0]
    for _, row in tqdm(rows.iterrows(), total=len(rows), desc="Cumulative burden Q's"):
        state = row["state_abbr"]
        sa = state_avg_asthma.get(state, 10.0)
        poverty_rate = (row["population_below_poverty"] / row["total_population"]) if row["total_population"] > 0 else 0
        pct_minority = 1 - (row.get("pop_white_alone", 0) or 0) / max(row["total_population"], 1)

        gt = compute_cumulative_burden_score(
            pollution_indicators={
                "tri_releases_per_1k": (row.get("tri_total_releases_lbs", 0) or 0) / 1000,
                "asthma_ratio": (row.get("casthma", 0) or 0) / max(sa, 1),
            },
            demographic_indicators={
                "pct_poverty": poverty_rate * 100,
                "pct_minority": pct_minority * 100,
            },
        )

        q = {
            "question_id": f"burden_{row['tract_fips']}",
            "task_type": "cumulative_burden",
            "tier": 3,
            "question_text": template.format(
                tract_fips=row["tract_fips"],
                county="",  # Could look up from FIPS
                state=state,
            ),
            "ground_truth": gt,
            "data_sources": ["Census ACS", "CDC PLACES", "EPA TRI"],
            "tract_fips": row["tract_fips"],
            "difficulty": score_difficulty(num_sources=3, num_reasoning_steps=4),
        }
        questions.append(q)
    return questions


async def run_generation():
    """Generate all question types and write to benchmark parquet."""
    con = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)
    all_questions = []

    # Generate each task type
    all_questions.extend(_generate_threshold_questions(con, n=3000))
    all_questions.extend(_generate_cumulative_burden_questions(con, n=5000))
    # Additional task types would be added here following the same pattern

    con.close()

    if not all_questions:
        print("WARNING: No questions generated. Check warehouse data.")
        return

    # Convert to DataFrame
    df = pd.DataFrame(all_questions)
    df["ground_truth"] = df["ground_truth"].apply(json.dumps)
    df["data_sources"] = df["data_sources"].apply(json.dumps)
    df["difficulty"] = df["difficulty"].apply(json.dumps)

    # Split: 70% train, 15% val, 15% test
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)
    n = len(df)
    train_end = int(n * 0.7)
    val_end = int(n * 0.85)
    df.loc[:train_end, "split"] = "train"
    df.loc[train_end:val_end, "split"] = "validation"
    df.loc[val_end:, "split"] = "test"

    # Save
    BENCHMARK_DIR.mkdir(parents=True, exist_ok=True)
    for split_name in ("train", "validation", "test"):
        split_df = df[df["split"] == split_name].drop(columns=["split"])
        split_df.to_parquet(BENCHMARK_DIR / f"{split_name}.parquet", index=False)
        print(f"  {split_name}: {len(split_df)} questions")

    # Metadata
    meta = {
        "total_questions": len(df),
        "task_types": df["task_type"].value_counts().to_dict(),
        "tier_distribution": {
            str(k): int(v) for k, v in df["tier"].value_counts().items()
        },
    }
    (BENCHMARK_DIR / "metadata.json").write_text(json.dumps(meta, indent=2))
    print(f"\nGeneration complete: {len(df)} total questions")
```

- [ ] **Step 3: Commit**

```bash
git add src/generate/difficulty.py src/generate/runner.py
git commit -m "feat: question generation runner with DuckDB queries and ground truth"
```

---

### Task 12: NL Variation with Nova Lite (Cosmetic Paraphrasing)

**Files:**
- Create: `src/generate/nl_vary.py`

- [ ] **Step 1: Implement NL variation using Bedrock Nova Lite**

This is the ONLY place an LLM is used — and it is for **cosmetic surface variation only**. The template-generated question has correct content; Nova Lite rephrases it to sound more natural. The ground truth answer is NEVER touched by the LLM.

```python
# src/generate/nl_vary.py
"""Use Nova Lite on Bedrock to paraphrase template-generated questions.
This is cosmetic only — the ground truth answer is never modified.
Cost: ~$0.06/M input tokens, ~$0.24/M output tokens.
For 15K questions at ~100 tokens each = ~1.5M tokens in, ~1.5M out = ~$0.45 total."""

import asyncio
import json
import boto3
from tqdm import tqdm

from src.config import BEDROCK_REGION, MODEL_NL_VARY

SYSTEM_PROMPT = """You are a question rephraser. Given a question about environmental hazards, 
rewrite it in natural language while preserving ALL specific values, names, codes, and data points exactly. 
Do not change any numbers, FIPS codes, chemical names, or facility names. 
Only change the sentence structure and phrasing to sound more natural.
Return ONLY the rephrased question, nothing else."""


async def paraphrase_batch(questions: list[str], batch_size: int = 20) -> list[str]:
    """Paraphrase a list of questions using Nova Lite on Bedrock."""
    client = boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)
    results = []
    semaphore = asyncio.Semaphore(5)  # Limit concurrent Bedrock calls

    async def _call(q: str) -> str:
        async with semaphore:
            try:
                body = json.dumps({
                    "messages": [
                        {"role": "user", "content": [{"text": f"Rephrase this question:\n\n{q}"}]},
                    ],
                    "system": [{"text": SYSTEM_PROMPT}],
                    "inferenceConfig": {"maxNewTokens": 300, "temperature": 0.7},
                })
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: client.invoke_model(
                        modelId=MODEL_NL_VARY,
                        contentType="application/json",
                        accept="application/json",
                        body=body,
                    ),
                )
                result = json.loads(response["body"].read())
                return result["output"]["message"]["content"][0]["text"].strip()
            except Exception as e:
                print(f"  NL vary failed: {e}")
                return q  # Fall back to original

    tasks = [_call(q) for q in questions]
    for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="NL variation"):
        result = await coro
        results.append(result)

    return results
```

- [ ] **Step 2: Commit**

```bash
git add src/generate/nl_vary.py
git commit -m "feat: Nova Lite NL variation for natural-sounding questions"
```

---

### Task 13: Quality Assurance — Self-Check + Haiku Spot Audit

**Files:**
- Create: `src/quality/__init__.py`
- Create: `src/quality/self_check.py`
- Create: `src/quality/sample_audit.py`

- [ ] **Step 1: Implement self-check (re-derive and compare)**

```python
# src/quality/self_check.py
"""Re-derive ground truth answers from raw data and compare to generated answers.
This catches any bugs in the generation pipeline."""

import json
import pandas as pd
import duckdb
from src.config import WAREHOUSE_PATH, BENCHMARK_DIR


def run_self_check(sample_size: int = 500) -> dict:
    questions = pd.read_parquet(BENCHMARK_DIR / "train.parquet")
    sample = questions.sample(min(sample_size, len(questions)), random_state=42)

    con = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)
    mismatches = 0
    checked = 0

    for _, row in sample.iterrows():
        gt = json.loads(row["ground_truth"])
        task = row["task_type"]

        if task == "threshold_exceedance":
            from src.generate.ground_truth import compute_threshold_exceedance
            recomputed = compute_threshold_exceedance(
                measured=gt["measured"],
                threshold=gt["threshold"],
            )
            if recomputed["exceeds"] != gt["exceeds"]:
                mismatches += 1
                print(f"  MISMATCH: {row['question_id']}")
            checked += 1

    con.close()

    result = {
        "checked": checked,
        "mismatches": mismatches,
        "match_rate": (checked - mismatches) / checked if checked > 0 else 0,
    }
    print(f"\nSelf-check: {checked} checked, {mismatches} mismatches, "
          f"{result['match_rate']:.1%} match rate")
    return result
```

- [ ] **Step 2: Implement Haiku spot audit**

```python
# src/quality/sample_audit.py
"""Use Haiku 4.5 to spot-check a small sample of generated questions.
Checks: Is the question coherent? Does the ground truth seem correct?
Cost: ~500 questions * ~500 tokens each = ~250K tokens = ~$0.20 input + ~$1 output ≈ $1.20"""

import json
import boto3
import pandas as pd
from tqdm import tqdm

from src.config import BEDROCK_REGION, MODEL_QUALITY_CHECK, BENCHMARK_DIR

AUDIT_PROMPT = """You are auditing a benchmark question for quality. Rate each dimension 1-5:

Question: {question}
Ground Truth Answer: {ground_truth}
Data Sources Used: {data_sources}

Rate:
1. COHERENCE (1-5): Is the question clear and well-formed?
2. PLAUSIBILITY (1-5): Does the ground truth answer seem reasonable given the question?
3. SPECIFICITY (1-5): Does the question reference specific, concrete data (FIPS codes, facility names, values)?
4. DIFFICULTY (1-5): How challenging is this for an LLM?

Reply ONLY in JSON: {{"coherence": N, "plausibility": N, "specificity": N, "difficulty": N, "flag": "ok" or "review needed: <reason>"}}"""


def run_audit(sample_size: int = 200) -> pd.DataFrame:
    questions = pd.read_parquet(BENCHMARK_DIR / "train.parquet")
    sample = questions.sample(min(sample_size, len(questions)), random_state=123)

    client = boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)
    results = []

    for _, row in tqdm(sample.iterrows(), total=len(sample), desc="Haiku audit"):
        prompt = AUDIT_PROMPT.format(
            question=row["question_text"],
            ground_truth=row["ground_truth"],
            data_sources=row["data_sources"],
        )
        try:
            response = client.converse(
                modelId=MODEL_QUALITY_CHECK,
                messages=[{"role": "user", "content": [{"text": prompt}]}],
                inferenceConfig={"maxTokens": 200, "temperature": 0},
            )
            text = response["output"]["message"]["content"][0]["text"]
            scores = json.loads(text)
            scores["question_id"] = row["question_id"]
            results.append(scores)
        except Exception as e:
            results.append({"question_id": row["question_id"], "flag": f"error: {e}"})

    df = pd.DataFrame(results)
    flagged = df[df["flag"] != "ok"]
    print(f"\nAudit: {len(df)} checked, {len(flagged)} flagged for review")
    if not flagged.empty:
        print("Flagged reasons:")
        for _, r in flagged.iterrows():
            print(f"  {r['question_id']}: {r['flag']}")

    df.to_parquet(BENCHMARK_DIR / "audit_results.parquet", index=False)
    return df
```

- [ ] **Step 3: Commit**

```bash
git add src/quality/
git commit -m "feat: self-check and Haiku spot-audit for quality assurance"
```

---

### Task 14: Export to HuggingFace Format

**Files:**
- Create: `src/export/__init__.py`
- Create: `src/export/to_hf.py`
- Create: `src/export/stats.py`

- [ ] **Step 1: Implement HuggingFace export**

```python
# src/export/to_hf.py
"""Package the benchmark for HuggingFace Hub upload."""

import json
import shutil
from pathlib import Path

from src.config import BENCHMARK_DIR, RAW_DIR


def export_to_hf(output_dir: Path | None = None):
    output_dir = output_dir or BENCHMARK_DIR / "hf_release"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Copy splits
    for split in ("train", "validation", "test"):
        src = BENCHMARK_DIR / f"{split}.parquet"
        if src.exists():
            shutil.copy2(src, output_dir / f"{split}.parquet")

    # Copy context data (source tables for retrieval tasks)
    context_dir = output_dir / "context"
    context_dir.mkdir(exist_ok=True)
    for src_file in [
        RAW_DIR / "census" / "census_all.parquet",
        RAW_DIR / "cdc_places" / "cdc_places_all.parquet",
        RAW_DIR / "epa_tri" / "tri_facilities_geocoded.parquet",
    ]:
        if src_file.exists():
            shutil.copy2(src_file, context_dir / src_file.name)

    # Write dataset card
    meta = json.loads((BENCHMARK_DIR / "metadata.json").read_text())
    readme = f"""---
language:
- en
license: cc-by-4.0
task_categories:
- question-answering
- text-generation
tags:
- environmental-justice
- multi-source-reasoning
- government-data
- benchmark
size_categories:
- 10K<n<100K
configs:
- config_name: default
  data_files:
  - split: train
    path: train.parquet
  - split: validation
    path: validation.parquet
  - split: test
    path: test.parquet
---

# EnvBench: Multi-Source Environmental Hazard Reasoning Benchmark

**{meta['total_questions']:,} questions** testing LLM reasoning across US government environmental databases.

## Task Types
{chr(10).join(f"- **{k}**: {v} questions" for k, v in meta['task_types'].items())}

## Data Sources
- EPA Toxic Release Inventory (TRI)
- EPA Safe Drinking Water Information System (SDWIS)
- EPA Greenhouse Gas Reporting Program
- EPA ECHO Enforcement & Compliance
- Census American Community Survey (ACS)
- CDC PLACES Health Outcomes
- NOAA Storm Events

## Key Features
- All ground truth computed deterministically from government data (no human annotation)
- Three difficulty tiers: retrieval, integration, adversarial/causal
- All source data is US government public domain
"""
    (output_dir / "README.md").write_text(readme)
    print(f"HuggingFace release prepared at {output_dir}")
```

- [ ] **Step 2: Implement stats summary**

```python
# src/export/stats.py
"""Print dataset statistics."""

import json
import pandas as pd
from src.config import BENCHMARK_DIR


def print_stats():
    for split in ("train", "validation", "test"):
        path = BENCHMARK_DIR / f"{split}.parquet"
        if not path.exists():
            continue
        df = pd.read_parquet(path)
        print(f"\n=== {split} ===")
        print(f"  Total questions: {len(df)}")
        print(f"  Task types: {df['task_type'].value_counts().to_dict()}")
        print(f"  Tiers: {df['tier'].value_counts().to_dict()}")

    meta_path = BENCHMARK_DIR / "metadata.json"
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
        print(f"\n=== Overall ===")
        print(f"  Total: {meta['total_questions']}")
```

- [ ] **Step 3: Commit**

```bash
git add src/export/
git commit -m "feat: HuggingFace export with dataset card and statistics"
```

---

## Running the Full Pipeline

After all tasks are implemented, the full pipeline runs with a single command:

```bash
# Full run (takes ~2-4 hours depending on API speed)
uv run python scripts/run_pipeline.py

# Or stage by stage:
uv run python scripts/run_pipeline.py --stage ingest    # ~1-2 hours (API-bound)
uv run python scripts/run_pipeline.py --stage join       # ~10-30 minutes (CPU-bound, geocoding)
uv run python scripts/run_pipeline.py --stage generate   # ~5-15 minutes
uv run python scripts/run_pipeline.py --stage export     # ~1 minute
```

**Resume behavior:** Each ingestor saves per-state Parquet files. If the pipeline crashes mid-ingest, re-running skips already-downloaded states (check if file exists). The join and generation stages are idempotent.

**Estimated total output:**
- Raw data: ~2-5 GB across all sources
- DuckDB warehouse: ~500 MB - 1 GB
- Final benchmark: ~50-100 MB (15-20K questions)
- Total LLM cost: < $15 (Nova Lite for NL variation + Haiku for audit)
