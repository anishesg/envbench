import json
import shutil
from pathlib import Path

from src.config import BENCHMARK_DIR, RAW_DIR


def export_to_hf(output_dir: Path | None = None):
    output_dir = output_dir or BENCHMARK_DIR / "hf_release"
    output_dir.mkdir(parents=True, exist_ok=True)

    for split in ("train", "validation", "test"):
        src = BENCHMARK_DIR / f"{split}.parquet"
        if src.exists():
            shutil.copy2(src, output_dir / f"{split}.parquet")

    context_dir = output_dir / "context"
    context_dir.mkdir(exist_ok=True)
    for src_file in [
        RAW_DIR / "census" / "census_all.parquet",
        RAW_DIR / "cdc_places" / "cdc_places_all.parquet",
        RAW_DIR / "epa_tri" / "tri_facilities_geocoded.parquet",
    ]:
        if src_file.exists():
            shutil.copy2(src_file, context_dir / src_file.name)

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
