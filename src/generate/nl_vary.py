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
    client = boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)
    results = []
    semaphore = asyncio.Semaphore(5)

    async def _call(q: str) -> str:
        async with semaphore:
            try:
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: client.converse(
                        modelId=MODEL_NL_VARY,
                        messages=[{"role": "user", "content": [{"text": f"Rephrase this question:\n\n{q}"}]}],
                        system=[{"text": SYSTEM_PROMPT}],
                        inferenceConfig={"maxTokens": 300, "temperature": 0.7},
                    ),
                )
                return response["output"]["message"]["content"][0]["text"].strip()
            except Exception as e:
                print(f"  NL vary failed: {e}")
                return q

    tasks = [_call(q) for q in questions]
    for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="NL variation"):
        result = await coro
        results.append(result)

    return results
