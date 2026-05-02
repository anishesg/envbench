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
