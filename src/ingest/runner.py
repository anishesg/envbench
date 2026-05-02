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
