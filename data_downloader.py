import argparse
import asyncio
import json
import logging
import os

import aiohttp
import pandas as pd
from tqdm.asyncio import tqdm


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

API_URL = "https://api2.suhail.ai/parcel/buildingRules?parcelObjectId={}"
CONCURRENT_LIMIT = 40
SAVE_EVERY = 1000
MAX_RETRIES = 5

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://suhail.ai/",
    "Connection": "keep-alive",
}

FINAL_COLUMNS = [
    "parcel_id",
    "parcel_objectid",
    "lon",
    "lat",
    "rule_id",
    "zoningId",
    "zoningColor",
    "zoningGroup",
    "landuse",
    "description",
    "name",
    "coloring",
    "coloringDescription",
    "maxBuildingCoefficient",
    "maxBuildingHeight",
    "maxParcelCoverage",
    "maxRuleDepth",
    "mainStreetsSetback",
    "secondaryStreetsSetback",
    "sideRearSetback",
    "api_status",
    "extra_data",
]
KNOWN_COLUMNS_SET = set(FINAL_COLUMNS)


async def fetch_parcel(session, row, semaphore):
    async with semaphore:
        pid = str(row.get("parcel_objectid", "")).strip()
        if pid.endswith(".0"):
            pid = pid[:-2]

        url = API_URL.format(pid)
        base_row = {key: row.get(key) for key in row if key in KNOWN_COLUMNS_SET}

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with session.get(url, timeout=20) as response:
                    if response.status == 200:
                        try:
                            json_data = await response.json()
                        except Exception:
                            base_row["api_status"] = "JSON_ERROR"
                            return [base_row]

                        data_list = json_data.get("data", [])
                        if not data_list:
                            base_row["api_status"] = "NO_DATA_IN_LIST"
                            return [base_row]

                        generated_rows = []
                        for info in data_list:
                            new_row = base_row.copy()
                            extra_data_bucket = {}
                            for key, value in info.items():
                                target_key = "rule_id" if key == "id" else key
                                if target_key in KNOWN_COLUMNS_SET:
                                    new_row[target_key] = value
                                else:
                                    extra_data_bucket[key] = value

                            if extra_data_bucket:
                                new_row["extra_data"] = json.dumps(extra_data_bucket, ensure_ascii=False)
                            new_row["api_status"] = "SUCCESS"
                            generated_rows.append(new_row)
                        return generated_rows

                    if response.status == 404:
                        base_row["api_status"] = "NOT_FOUND"
                        return [base_row]

                    if response.status == 429:
                        await asyncio.sleep(2 * attempt)
                    else:
                        await asyncio.sleep(1)

            except Exception as exc:
                if attempt == MAX_RETRIES:
                    logging.debug("Failed parcel %s after retries: %s", pid, exc)
                await asyncio.sleep(1)

        base_row["api_status"] = "FAILED_AFTER_RETRIES"
        return [base_row]


def load_resume_state(input_csv: str, output_csv: str):
    if not os.path.exists(input_csv):
        raise FileNotFoundError(f"Input file not found: {input_csv}")

    df_input = pd.read_csv(input_csv, dtype={"parcel_objectid": str, "parcel_id": str})
    df_input = df_input.dropna(subset=["parcel_objectid"])

    processed_ids = set()
    write_header = True
    if os.path.exists(output_csv):
        try:
            df_done = pd.read_csv(output_csv, usecols=["parcel_objectid"], dtype={"parcel_objectid": str})
            processed_ids = set(df_done["parcel_objectid"].dropna().tolist())
            write_header = False
            logging.info("Found %s processed parcels. Resuming.", len(processed_ids))
        except Exception:
            write_header = not (os.path.exists(output_csv) and os.path.getsize(output_csv) > 0)

    parcels_to_do = df_input[~df_input["parcel_objectid"].isin(processed_ids)].to_dict("records")
    logging.info("Remaining parcels: %s", len(parcels_to_do))
    return parcels_to_do, write_header


def write_chunk(rows, output_csv: str, write_header: bool) -> bool:
    df_chunk = pd.DataFrame(rows).reindex(columns=FINAL_COLUMNS)
    df_chunk.to_csv(
        output_csv,
        index=False,
        mode="w" if write_header else "a",
        header=write_header,
        encoding="utf-8-sig",
    )
    return False


async def run_downloader(input_csv: str, output_csv: str, concurrent_limit: int = CONCURRENT_LIMIT):
    parcels_to_do, write_header = load_resume_state(input_csv, output_csv)
    if not parcels_to_do:
        logging.info("Job is already complete.")
        return

    semaphore = asyncio.Semaphore(concurrent_limit)
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        tasks = [fetch_parcel(session, row, semaphore) for row in parcels_to_do]
        results_buffer = []

        for future in tqdm.as_completed(tasks, total=len(tasks), desc="Downloading rules"):
            result = await future
            results_buffer.extend(result)

            if len(results_buffer) >= SAVE_EVERY:
                write_header = write_chunk(results_buffer, output_csv, write_header)
                results_buffer = []

        if results_buffer:
            write_chunk(results_buffer, output_csv, write_header)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download parcel-level building rules from Suhail API.")
    parser.add_argument("--input", default="riyadh_parcels_centroids.csv")
    parser.add_argument("--output", default="riyadh_parcels_full_data.csv")
    parser.add_argument("--concurrency", type=int, default=CONCURRENT_LIMIT)
    args = parser.parse_args()

    asyncio.run(run_downloader(args.input, args.output, args.concurrency))
    logging.info("Complete. Data saved to %s", args.output)


if __name__ == "__main__":
    main()
