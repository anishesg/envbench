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
