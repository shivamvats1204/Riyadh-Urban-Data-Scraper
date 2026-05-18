import argparse
import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

INPUT_FILE = "riyadh_parcels_full_data.csv"
OUTPUT_FILE = "riyadh_parcels_engineered.csv"

VALID_API_STATUSES = {
    "SUCCESS",
    "NO_DATA_IN_LIST",
    "NOT_FOUND",
    "JSON_ERROR",
    "FAILED_AFTER_RETRIES",
    "ERROR_CONNECTION",
}

NUMERIC_FEATURES = [
    "maxBuildingCoefficient",
    "maxParcelCoverage",
    "maxRuleDepth",
    "mainStreetsSetback",
    "secondaryStreetsSetback",
    "sideRearSetback",
]

FEATURE_BOUNDS = {
    "maxBuildingCoefficient": (0, 20),
    "maxParcelCoverage": (0, 100),
    "maxRuleDepth": (0, 500),
    "mainStreetsSetback": (0, 100),
    "secondaryStreetsSetback": (0, 100),
    "sideRearSetback": (0, 100),
}

NUMBER_RE = re.compile(r"\d+(?:\.\d+)?")


def parse_regulatory_number(value, strategy: str = "max") -> float:
    """Extract a defensible number from mixed Arabic/English regulation text.

    Ranges like "60-75" become 75 for upper-bound planning constraints instead
    of the old, damaging "6075" behavior.
    """
    if pd.isna(value):
        return np.nan
    if isinstance(value, (int, float, np.number)):
        return float(value)

    text = str(value).strip()
    if not text:
        return np.nan

    numbers = [float(item) for item in NUMBER_RE.findall(text)]
    if not numbers:
        return np.nan
    if strategy == "mean":
        return float(np.mean(numbers))
    if strategy == "min":
        return float(np.min(numbers))
    return float(np.max(numbers))


def flag_malformed_statuses(df: pd.DataFrame) -> pd.DataFrame:
    if "api_status" not in df.columns:
        df["Data_Quality_Flag"] = "MISSING_STATUS_COLUMN"
        return df

    status = df["api_status"].astype("string")
    malformed = status.notna() & ~status.isin(VALID_API_STATUSES)
    df["Original_Api_Status"] = df["api_status"]
    df.loc[malformed, "api_status"] = "MALFORMED_OR_SHIFTED"
    df["Data_Quality_Flag"] = np.where(malformed, "REVIEW_SOURCE_ROW", "OK")
    logging.info("Flagged %s rows with malformed api_status values.", int(malformed.sum()))
    return df


def clean_and_prepare_data(input_csv: str) -> pd.DataFrame:
    logging.info("Loading data from %s...", input_csv)
    df = pd.read_csv(input_csv)
    df = flag_malformed_statuses(df)

    for col in NUMERIC_FEATURES:
        if col not in df.columns:
            logging.warning("Expected numeric column missing: %s", col)
            df[col] = np.nan
            continue

        df[col] = df[col].map(parse_regulatory_number)
        lower, upper = FEATURE_BOUNDS[col]
        invalid = (df[col] < lower) | (df[col] > upper)
        if invalid.any():
            logging.info("Nulling %s out-of-bound values in %s.", int(invalid.sum()), col)
            df.loc[invalid, col] = np.nan

    if "zoningGroup" in df.columns:
        for col in NUMERIC_FEATURES:
            df[col] = df.groupby("zoningGroup", dropna=False)[col].transform(
                lambda series: series.fillna(series.median())
            )

    for col in NUMERIC_FEATURES:
        df[col] = df[col].fillna(df[col].median()).fillna(0)

    logging.info("Data cleaning and regulatory feature extraction complete.")
    return df


def apply_mathematical_model(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Applying development potential model...")

    total_setback = (
        df["mainStreetsSetback"] + df["secondaryStreetsSetback"] + df["sideRearSetback"]
    )
    coverage_ratio = df["maxParcelCoverage"] / 100.0

    df["Total_Setback"] = total_setback
    df["DPI_Raw"] = df["maxBuildingCoefficient"] * coverage_ratio

    max_setback = total_setback.max()
    setback_penalty = np.where(max_setback > 0, (total_setback / max_setback) * 0.1, 0)
    df["Development_Potential_Index"] = df["DPI_Raw"] * (1 - setback_penalty)

    max_val = df["Development_Potential_Index"].max()
    df["Normalized_DPI"] = (
        (df["Development_Potential_Index"] / max_val) * 100 if max_val > 0 else 0
    )
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean Riyadh parcel regulations and build DPI features.")
    parser.add_argument("--input", default=INPUT_FILE)
    parser.add_argument("--output", default=OUTPUT_FILE)
    args = parser.parse_args()

    data = clean_and_prepare_data(args.input)
    final_data = apply_mathematical_model(data)
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    final_data.to_csv(args.output, index=False, encoding="utf-8")
    logging.info("Saved engineered dataset to %s", args.output)


if __name__ == "__main__":
    main()
