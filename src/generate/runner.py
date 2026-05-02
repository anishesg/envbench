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
                county="",
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
    con = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)
    all_questions = []

    all_questions.extend(_generate_threshold_questions(con, n=3000))
    all_questions.extend(_generate_cumulative_burden_questions(con, n=5000))

    con.close()

    if not all_questions:
        print("WARNING: No questions generated. Check warehouse data.")
        return

    df = pd.DataFrame(all_questions)
    df["ground_truth"] = df["ground_truth"].apply(json.dumps)
    df["data_sources"] = df["data_sources"].apply(json.dumps)
    df["difficulty"] = df["difficulty"].apply(json.dumps)

    df = df.sample(frac=1, random_state=42).reset_index(drop=True)
    n = len(df)
    train_end = int(n * 0.7)
    val_end = int(n * 0.85)
    df.loc[:train_end, "split"] = "train"
    df.loc[train_end:val_end, "split"] = "validation"
    df.loc[val_end:, "split"] = "test"

    BENCHMARK_DIR.mkdir(parents=True, exist_ok=True)
    for split_name in ("train", "validation", "test"):
        split_df = df[df["split"] == split_name].drop(columns=["split"])
        split_df.to_parquet(BENCHMARK_DIR / f"{split_name}.parquet", index=False)
        print(f"  {split_name}: {len(split_df)} questions")

    meta = {
        "total_questions": len(df),
        "task_types": df["task_type"].value_counts().to_dict(),
        "tier_distribution": {
            str(k): int(v) for k, v in df["tier"].value_counts().items()
        },
    }
    (BENCHMARK_DIR / "metadata.json").write_text(json.dumps(meta, indent=2))
    print(f"\nGeneration complete: {len(df)} total questions")
