import hashlib
import math
import re
import unicodedata

import numpy as np
import pandas as pd

FIELDS = {
    "parcel_id": "Location / parcel ID", "zoningGroup": "Zone / category", "landuse": "Land use",
    "lat": "Latitude", "lon": "Longitude", "description": "Description / rules",
    "maxBuildingCoefficient": "Floor area ratio (FAR)", "maxParcelCoverage": "Coverage (%)",
    "mainStreetsSetback": "Main street setback (m)", "secondaryStreetsSetback": "Secondary setback (m)",
    "sideRearSetback": "Side / rear setback (m)", "Normalized_DPI": "Development potential index",
    "Anomaly_Score_MSE": "Anomaly score", "Data_Quality_Flag": "Data quality",
}
NUMERIC = {"lat", "lon", "maxBuildingCoefficient", "maxParcelCoverage", "mainStreetsSetback",
           "secondaryStreetsSetback", "sideRearSetback", "Normalized_DPI", "Anomaly_Score_MSE"}
CATEGORICAL = {"parcel_id", "zoningGroup", "landuse", "Data_Quality_Flag"}


def number(value):
    if pd.isna(value):
        return np.nan
    text = unicodedata.normalize("NFKC", str(value)).replace("٫", ".").replace("٬", "")
    try:
        result = float(text)
        return result if math.isfinite(result) else np.nan
    except ValueError:
        return np.nan  # Keep conditional rules/ranges in their original fields; do not guess.


def adapt(frame, mapping=None, source_name="dataset"):
    """Keep all original columns and add canonical fields with row-level evidence IDs."""
    if frame.empty or len(frame) > 1_000_000:
        raise ValueError("Dataset must contain 1–1,000,000 rows")
    frame = frame.copy()
    if frame.columns.duplicated().any():
        raise ValueError("Duplicate column names are not supported")
    mapping = mapping or {key: key for key in FIELDS if key in frame}
    if "parcel_id" not in mapping:
        raise ValueError("Map an ID column before opening the dataset")
    # Snapshot prevents swapped field mappings from reading already-overwritten columns.
    original = frame.copy()
    for field in FIELDS:
        if field in frame and mapping.get(field) != field:
            preserved = f"original_{field}"
            if preserved in frame:
                raise ValueError(f"Reserved mapping column already exists: {preserved}")
            frame = frame.rename(columns={field: preserved})
    for canonical, source in mapping.items():
        if canonical not in FIELDS or source not in original:
            raise ValueError("Unknown field mapping")
        if canonical in NUMERIC:
            frame[f"raw_{canonical}"] = original[source]
            frame[canonical] = original[source].map(number)
        else:
            frame[canonical] = original[source].astype("string")
    if frame["parcel_id"].isna().any() or frame["parcel_id"].astype(str).str.strip().eq("").any():
        raise ValueError("Every record must have an ID")
    frame["parcel_id"] = frame["parcel_id"].astype(str)
    for col, lower, upper in [("lat", -90, 90), ("lon", -180, 180), ("maxParcelCoverage", 0, 100)]:
        if col in frame:
            frame.loc[~frame[col].between(lower, upper), col] = np.nan
    if "Data_Quality_Flag" not in frame:
        frame["Data_Quality_Flag"] = "UNVERIFIED_SOURCE"
    invalid_numeric = pd.Series(False, index=frame.index)
    for field in NUMERIC:
        if field in mapping:
            raw_values = original[mapping[field]]
            invalid_numeric |= raw_values.notna() & raw_values.astype("string").str.strip().ne("").fillna(False) & frame[field].isna()
    frame.loc[invalid_numeric & frame["Data_Quality_Flag"].eq("OK"), "Data_Quality_Flag"] = "NUMERIC_PARSE_REVIEW"
    digest = hashlib.sha256(pd.util.hash_pandas_object(original, index=True).values.tobytes()).hexdigest()[:10]
    frame["source_record_id"] = [f"{digest}:row-{i + 1}" for i in range(len(frame))]
    frame["source_dataset"] = source_name
    return frame.reset_index(drop=True)


def profile(frame):
    return {
        "records": len(frame), "unique_locations": int(frame["parcel_id"].nunique()),
        "multiple_records_per_location": bool(frame["parcel_id"].duplicated().any()),
        "fields": {key: {"label": FIELDS[key], "missing": int(frame[key].isna().sum())}
                   for key in FIELDS if key in frame},
        "categories": {key: sorted(frame[key].dropna().astype(str).unique().tolist())[:40]
                       for key in ("zoningGroup", "landuse") if key in frame},
    }
