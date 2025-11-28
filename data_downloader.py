import pandas as pd
import requests
import concurrent.futures
from tqdm import tqdm
import time
import os
import json

# -------------------------------
# CONFIGURATION
# -------------------------------
INPUT_CSV = "riyadh_parcels_centroids.csv"         # The source of IDs
OUTPUT_CSV = "riyadh_parcels_full_data_final.csv"     # The file you are saving to
API_URL = "https://api2.suhail.ai/parcel/buildingRules?parcelObjectId={}"

MAX_WORKERS = 15
SAVE_EVERY = 1000

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Referer": "https://suhail.ai/"
}

# -------------------------------
# 1. SMART RESUME LOGIC
# -------------------------------
print("--- RESUMING JOB ---")

# A. Read the Source Input
print("Reading Input CSV...")
if not os.path.exists(INPUT_CSV):
    print(f"Error: Input file '{INPUT_CSV}' not found.")
    exit()

df_input = pd.read_csv(INPUT_CSV, dtype={'parcel_objectid': str, 'parcel_id': str})
df_input = df_input.dropna(subset=['parcel_objectid'])
total_parcels = len(df_input)
print(f"Total Source Parcels: {total_parcels}")

# B. Read the Existing Output (to see what's done)
processed_ids = set()

if os.path.exists(OUTPUT_CSV):
    print(f"Reading existing Output CSV to find completed rows...")
    try:
        # Read only the 'parcel_objectid' column to save memory
        df_done = pd.read_csv(OUTPUT_CSV, usecols=['parcel_objectid'], dtype={'parcel_objectid': str})
        processed_ids = set(df_done['parcel_objectid'].tolist())
        print(f"✅ Found {len(processed_ids)} parcels already completed.")
    except ValueError:
        print("⚠️ Warning: Output CSV exists but might be empty or missing 'parcel_objectid' column.")
        print("Starting from zero for safety (or check your file).")
else:
    print("No existing output file found. Starting from scratch.")

# C. Filter: Keep only what is NOT done
# We use a lambda to check if the ID is NOT in the processed_ids set
parcels_to_do = df_input[~df_input['parcel_objectid'].isin(processed_ids)].to_dict('records')

remaining_count = len(parcels_to_do)
print(f"------------------------------------------------")
print(f"Skipping: {len(processed_ids)}")
print(f"REMAINING: {remaining_count}")
print(f"------------------------------------------------")

if remaining_count == 0:
    print("🎉 Job is already 100% complete! Nothing to do.")
    exit()

# -------------------------------
# 2. FETCH FUNCTION
# -------------------------------
def fetch_parcel_data(row):
    pid = str(row.get('parcel_objectid', '')).strip()
    if pid.endswith('.0'):
        pid = pid[:-2]

    url = API_URL.format(pid)

    # Initialize fields
    new_cols = {
        'landuse': None, 'zoningGroup': None, 'description': None,
        'coloringDescription': None, 'maxBuildingHeight': None,
        'maxParcelCoverage': None, 'maxBuildingCoefficient': None,
        'mainStreetsSetback': None, 'sideRearSetback': None,
        'zoningId': None, 'api_status': "FAILED"
    }
    row.update(new_cols)

    try:
        response = requests.get(url, headers=HEADERS, timeout=10)

        if response.status_code == 200:
            json_data = response.json()
            data_list = json_data.get('data', [])

            if data_list and isinstance(data_list, list) and len(data_list) > 0:
                info = data_list[0]
                row['landuse'] = info.get('landuse')
                row['zoningGroup'] = info.get('zoningGroup')
                row['description'] = info.get('description')
                row['coloringDescription'] = info.get('coloringDescription')
                row['maxBuildingHeight'] = info.get('maxBuildingHeight')
                row['maxParcelCoverage'] = info.get('maxParcelCoverage')
                row['maxBuildingCoefficient'] = info.get('maxBuildingCoefficient')
                row['mainStreetsSetback'] = info.get('mainStreetsSetback')
                row['sideRearSetback'] = info.get('sideRearSetback')
                row['zoningId'] = info.get('zoningId')
                row['api_status'] = "SUCCESS"
            else:
                row['api_status'] = "NO_DATA_IN_LIST"

        elif response.status_code == 429:
            row['api_status'] = "RATE_LIMIT"
            time.sleep(2)
        elif response.status_code == 404:
             row['api_status'] = "NOT_FOUND"
        else:
            row['api_status'] = f"HTTP_{response.status_code}"

    except Exception as e:
        row['api_status'] = "ERROR_CONNECTION"

    return row

# -------------------------------
# 3. EXECUTION LOOP
# -------------------------------
print(f"Resuming with {MAX_WORKERS} workers...")

results_buffer = []

# We always append ('a') because we are resuming.
# We NEVER write the header=True because the file already has headers.
# Exception: If the file didn't exist (fresh start), we write headers.
write_header = not os.path.exists(OUTPUT_CSV)

with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = executor.map(fetch_parcel_data, parcels_to_do)

    for i, result in tqdm(enumerate(futures), total=len(parcels_to_do), desc="Processing"):
        results_buffer.append(result)

        if len(results_buffer) >= SAVE_EVERY:
            df_chunk = pd.DataFrame(results_buffer)

            # If we started from scratch (write_header=True), we write header on the first chunk ONLY
            if write_header:
                df_chunk.to_csv(OUTPUT_CSV, index=False, mode='w', encoding='utf-8-sig')
                write_header = False # Next chunks will append
            else:
                # Appending to existing file: header=False
                df_chunk.to_csv(OUTPUT_CSV, index=False, mode='a', header=False, encoding='utf-8-sig')

            results_buffer = []

# Save final buffer
if results_buffer:
    df_chunk = pd.DataFrame(results_buffer)
    if write_header:
        df_chunk.to_csv(OUTPUT_CSV, index=False, mode='w', encoding='utf-8-sig')
    else:
        df_chunk.to_csv(OUTPUT_CSV, index=False, mode='a', header=False, encoding='utf-8-sig')

print(f"\n✅ COMPLETE! All remaining data appended to: {OUTPUT_CSV}")