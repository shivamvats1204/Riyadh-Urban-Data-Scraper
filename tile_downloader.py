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
# Double check this filename matches your actual file on disk
INPUT_CSV = "riyadh_parcels_centroids.csv"
OUTPUT_CSV = "riyadh_parcels_full_data_final.csv"
API_URL = "https://api2.suhail.ai/parcel/buildingRules?parcelObjectId={}"

# WORKER SETTINGS
# 15 is a safe speed. If you get many "RATE_LIMIT" errors, lower to 10.
MAX_WORKERS = 50
SAVE_EVERY = 1000  # Write to disk every 1000 rows

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Referer": "https://suhail.ai/"
}

# -------------------------------
# 1. READ INPUT DATA
# -------------------------------
print("Reading input CSV (this may take a moment)...")
if not os.path.exists(INPUT_CSV):
    print(f"Error: Input file '{INPUT_CSV}' not found.")
    exit()

# Force IDs to strings to prevent scientific notation (E+11) issues
df_input = pd.read_csv(INPUT_CSV, dtype={'parcel_objectid': str, 'parcel_id': str})

# Remove rows with missing IDs
parcels = df_input.dropna(subset=['parcel_objectid']).to_dict('records')
print(f"Loaded {len(parcels)} parcels to process.")

# -------------------------------
# 2. FETCH FUNCTION
# -------------------------------
def fetch_parcel_data(row):
    # Prepare ID: clean string, remove decimals
    pid = str(row.get('parcel_objectid', '')).strip()
    if pid.endswith('.0'):
        pid = pid[:-2]

    url = API_URL.format(pid)

    # Initialize fields with None so columns align in CSV
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

                # Update row with extracted fields
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
            time.sleep(2) # Auto-throttle
        elif response.status_code == 404:
             row['api_status'] = "NOT_FOUND"
        else:
            row['api_status'] = f"HTTP_{response.status_code}"

    except Exception as e:
        row['api_status'] = "ERROR_CONNECTION"

    return row

# -------------------------------
# 3. MAIN EXECUTION (CHUNKED SAVING)
# -------------------------------
print(f"Starting FULL extraction with {MAX_WORKERS} workers...")
print(f"Progress will be saved to '{OUTPUT_CSV}' every {SAVE_EVERY} rows.")

results_buffer = []
file_initialized = False # Tracks if we have written the header yet

# Check if output file already exists (to avoid overwriting if you restart)
if os.path.exists(OUTPUT_CSV):
    print("⚠️ Warning: Output file already exists. New data will be appended.")
    # If resuming, you might want to load existing IDs and skip them,
    # but for simplicity, this script just appends.
    file_initialized = True

with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = executor.map(fetch_parcel_data, parcels)

    for i, result in tqdm(enumerate(futures), total=len(parcels), desc="Processing"):
        results_buffer.append(result)

        # SAVE BUFFER TO DISK
        if len(results_buffer) >= SAVE_EVERY:
            df_chunk = pd.DataFrame(results_buffer)

            # mode='a' means append
            # header=not file_initialized means write header only once at the top
            if not file_initialized:
                df_chunk.to_csv(OUTPUT_CSV, index=False, mode='w', encoding='utf-8-sig')
                file_initialized = True
            else:
                df_chunk.to_csv(OUTPUT_CSV, index=False, mode='a', header=False, encoding='utf-8-sig')

            # Clear memory
            results_buffer = []

# SAVE REMAINING ROWS (After loop finishes)
if results_buffer:
    df_chunk = pd.DataFrame(results_buffer)
    if not file_initialized:
        df_chunk.to_csv(OUTPUT_CSV, index=False, mode='w', encoding='utf-8-sig')
    else:
        df_chunk.to_csv(OUTPUT_CSV, index=False, mode='a', header=False, encoding='utf-8-sig')

print(f"\n✅ COMPLETE! All data saved to: {OUTPUT_CSV}")