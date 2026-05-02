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
