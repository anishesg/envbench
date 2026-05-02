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
