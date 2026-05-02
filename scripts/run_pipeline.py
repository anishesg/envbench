"""
Full pipeline: ingest -> join -> generate -> export.
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
