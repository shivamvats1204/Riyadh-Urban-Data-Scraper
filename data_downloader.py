import pandas as pd
import asyncio
import aiohttp
import os
import json
import time
from tqdm.asyncio import tqdm

# -------------------------------
# CONFIGURATION
# -------------------------------
# NOTE: Update these paths for your local machine!
# e.g., r"C:\Users\Suhail\Documents\riyadh_parcels.csv"
INPUT_CSV = r"riyadh_parcels_centroids_corrected_15 (1).csv"
OUTPUT_CSV = r"riyadh_parcels_full_data_final2.csv"
API_URL = "https://api2.suhail.ai/parcel/buildingRules?parcelObjectId={}"

# ASYNC SETTINGS
CONCURRENT_LIMIT = 40
SAVE_EVERY = 1000
MAX_RETRIES = 5

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Referer": "https://suhail.ai/",
    "Connection": "keep-alive"
}

# STRICT COLUMNS
FINAL_COLUMNS = [
    'parcel_id', 'parcel_objectid', 'lon', 'lat', 'rule_id',
    'zoningId', 'zoningColor', 'zoningGroup', 'landuse', 'description',
    'name', 'coloring', 'coloringDescription', 'maxBuildingCoefficient',
    'maxBuildingHeight', 'maxParcelCoverage', 'maxRuleDepth',
    'mainStreetsSetback', 'secondaryStreetsSetback', 'sideRearSetback',
    'api_status', 'extra_data'
]
KNOWN_COLUMNS_SET = set(FINAL_COLUMNS)

# -------------------------------
# 1. SETUP
# -------------------------------
print("--- STARTING ULTRA-FAST ASYNC SCRAPER ---")

if not os.path.exists(INPUT_CSV):
    print(f"Error: Input file '{INPUT_CSV}' not found.")
    print("Please update the INPUT_CSV path in the Configuration section.")
    exit()

df_input = pd.read_csv(INPUT_CSV, dtype={'parcel_objectid': str, 'parcel_id': str})
df_input = df_input.dropna(subset=['parcel_objectid'])

processed_ids = set()
write_header = True

if os.path.exists(OUTPUT_CSV):
    try:
        df_done = pd.read_csv(OUTPUT_CSV, usecols=['parcel_objectid'], dtype={'parcel_objectid': str})
        processed_ids = set(df_done['parcel_objectid'].tolist())
        write_header = False
        print(f"✅ Found {len(processed_ids)} processed parcels. Skipping.")
    except Exception:
        if os.path.exists(OUTPUT_CSV) and os.path.getsize(OUTPUT_CSV) > 0:
             write_header = False

parcels_to_do = df_input[~df_input['parcel_objectid'].isin(processed_ids)].to_dict('records')
print(f"Remaining Parcels: {len(parcels_to_do)}")

if len(parcels_to_do) == 0:
    print("🎉 Job is already complete!")
    exit()

# -------------------------------
# 2. ASYNC WORKER
# -------------------------------
async def fetch_parcel(session, row, semaphore):
    async with semaphore:
        pid = str(row.get('parcel_objectid', '')).strip()
        if pid.endswith('.0'):
            pid = pid[:-2]

        url = API_URL.format(pid)
        base_row = {k: row.get(k) for k in row if k in KNOWN_COLUMNS_SET}
        generated_rows = []

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with session.get(url, timeout=20) as response:
                    if response.status == 200:
                        try:
                            json_data = await response.json()
                        except:
                            base_row['api_status'] = "JSON_ERROR"
                            generated_rows.append(base_row)
                            return generated_rows

                        data_list = json_data.get('data', [])

                        if data_list and isinstance(data_list, list) and len(data_list) > 0:
                            for info in data_list:
                                new_row = base_row.copy()
                                extra_data_bucket = {}

                                for key, value in info.items():
                                    target_key = 'rule_id' if key == 'id' else key
                                    if target_key in KNOWN_COLUMNS_SET:
                                        new_row[target_key] = value
                                    else:
                                        extra_data_bucket[key] = value

                                if extra_data_bucket:
                                    new_row['extra_data'] = json.dumps(extra_data_bucket, ensure_ascii=False)

                                new_row['api_status'] = "SUCCESS"
                                generated_rows.append(new_row)
                            return generated_rows
                        else:
                            base_row['api_status'] = "NO_DATA_IN_LIST"
                            generated_rows.append(base_row)
                            return generated_rows

                    elif response.status == 404:
                        base_row['api_status'] = "NOT_FOUND"
                        generated_rows.append(base_row)
                        return generated_rows

                    elif response.status == 429:
                        await asyncio.sleep(2 * attempt)
                        continue
                    else:
                        await asyncio.sleep(1)
                        continue

            except Exception:
                await asyncio.sleep(1)
                continue

        base_row['api_status'] = "FAILED_AFTER_RETRIES"
        generated_rows.append(base_row)
        return generated_rows

# -------------------------------
# 3. MAIN MANAGER
# -------------------------------
async def main():
    global write_header
    semaphore = asyncio.Semaphore(CONCURRENT_LIMIT)

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        tasks = []
        for row in parcels_to_do:
            task = fetch_parcel(session, row, semaphore)
            tasks.append(task)

        results_buffer = []

        for f in tqdm.as_completed(tasks, total=len(tasks), desc="Async Speed"):
            result = await f
            results_buffer.extend(result)

            if len(results_buffer) >= SAVE_EVERY:
                df_chunk = pd.DataFrame(results_buffer)
                df_chunk = df_chunk.reindex(columns=FINAL_COLUMNS)

                if write_header:
                    df_chunk.to_csv(OUTPUT_CSV, index=False, mode='w', encoding='utf-8-sig')
                    write_header = False
                else:
                    df_chunk.to_csv(OUTPUT_CSV, index=False, mode='a', header=False, encoding='utf-8-sig')

                results_buffer = []

        if results_buffer:
            df_chunk = pd.DataFrame(results_buffer)
            df_chunk = df_chunk.reindex(columns=FINAL_COLUMNS)
            if write_header:
                df_chunk.to_csv(OUTPUT_CSV, index=False, mode='w', encoding='utf-8-sig')
            else:
                df_chunk.to_csv(OUTPUT_CSV, index=False, mode='a', header=False, encoding='utf-8-sig')

# -------------------------------
# 4. RUNNER (VS CODE / TERMINAL)
# -------------------------------
if __name__ == "__main__":
    # In standard Python scripts (VS Code, PyCharm, Terminal),
    # we use asyncio.run() to start the event loop.
    asyncio.run(main())
    print(f"\nCOMPLETE! Data saved to: {OUTPUT_CSV}")