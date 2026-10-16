# GeoAI — location intelligence, starting with Riyadh

A general geospatial analysis workspace with optional specialist settings. Riyadh remains the first source adapter and ML pipeline. Users can also upload another location dataset, map its columns, and use exact queries, comparisons, maps, and downloadable evidence briefs.

## Quick start

Python 3.12 is recommended. The application does not require PyTorch, LangChain, or an embedding-model download.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements-app.txt
python -m streamlit run app.py
```

Open the local URL printed by Streamlit. A small **synthetic** sample dataset is included; every demo location and constraint is fictional. If a scored/engineered Riyadh CSV exists in the repository directory, it is offered as a separate source. CSVs, keys, and generated model artifacts remain ignored by Git.

## General and advanced usage

- **Explore & query:** use a guided builder to filter records, calculate full-dataset aggregates, rank categories, and inspect a map.
- **Compare locations:** enter up to 10 IDs and export a reproducible brief containing the query, matching counts, and source records.
- **AI analyst:** ask a question, inspect the proposed query JSON, compute evidence, then optionally generate an explanation. Exact results remain visible independently of the generated text.
- **Data & model quality:** inspect missingness, original/raw values, quality flags, and an adjustable anomaly review overlay.
- **Advanced settings:** choose General, Urban planner, Property researcher, or Data analyst wording; English/Arabic explanations; model ID; result limit; and anomaly review percentile.
- **Custom datasets:** upload CSV (up to 50 MB/1 million rows), map an ID, and optionally map categories, descriptions, coordinates, and numeric planning fields. IDs are preserved as text. Coordinates must be WGS84 degrees. Unmapped domain fields remain preserved as original columns.

Use the builder, comparison, maps, and JSON query editor without a key. For model-assisted planning and explanations, create `.env` from `.env.example` and set `GEOAI_API_KEY`, or enter a Groq key in the session's password field. `GROQ_API_KEY` is also accepted. A server-admin `GEOAI_BASE_URL` can select another compatible endpoint. Do not commit `.env`.

Model planning receives a dataset profile (fields, counts, and sample categories); explanations receive computed results and a bounded record sample. Treat those as data sent to the chosen provider. Uploaded datasets are held in the active Streamlit session, not saved as repository files. Streamlit session state is not an authentication system; configure authentication and hosting controls before serving sensitive datasets to multiple users.

## Exact analysis contract

The LLM requests a small validated query language: `rows`, `aggregate`, `compare`, `search`, or `profile`. Python validates fields, operators, numeric arguments, and row limits. Model-generated Python and SQL are never executed.

Example:

```json
{
  "operation": "aggregate",
  "metric": "maxBuildingCoefficient",
  "aggregation": "mean",
  "group_by": "zoningGroup",
  "descending": true,
  "limit": 20
}
```

Supported aggregates: mean, min, max, sum, source-record count, and unique-location count. Filters support equality, literal contains, and numeric comparisons. Aggregates cover all matching records; result and map limits only limit what is displayed. Multi-rule locations remain separate records, and numeric means are record-weighted. Missing values are excluded, with non-missing counts reported. Raw conditional/range strings are preserved, not silently interpreted by the generalized adapter.

`source_record_id` combines a dataset-content fingerprint and row number. These IDs identify loaded evidence, not official documents. The current scraped source is Suhail; scraped records do not establish current authoritative regulation. DPI is a dataset-relative heuristic and anomaly scores indicate unusual patterns, not value, return, or a legal violation. Arabic output is configurable, but language quality still requires provider-specific evaluation.

## Riyadh ingestion and ML pipeline

Install `requirements.txt` for the full scraper, PyTorch, and SHAP workflow:

```powershell
python -m pip install -r requirements.txt
python tile_downloader.py --output riyadh_parcels_centroids.csv
python data_downloader.py --input riyadh_parcels_centroids.csv --output riyadh_parcels_full_data.csv
python phase1_data_pipeline.py --input riyadh_parcels_full_data.csv --output riyadh_parcels_engineered.csv
python phase2_pytorch_anomaly.py --input riyadh_parcels_engineered.csv --output riyadh_parcels_with_anomalies.csv
python phase3_explainable_ai.py --input riyadh_parcels_with_anomalies.csv
python -m streamlit run app.py
```

Phase 1 now retains raw numeric fields, flags imputation and multi-number parsing, and marks source-reviewed complete rows eligible for training. Known API failures no longer count as clean successful records. Existing range-to-upper-bound parsing remains in this adapter but is flagged for review. New custom datasets use the stricter generalized parser.

Phase 2 requires at least 20 eligible records, fits the scaler on the training split only, calibrates the anomaly threshold from held-out validation errors, and records that provenance in metadata. Ineligible rows are scored for review and labeled `Anomaly_Review_Only`; they do not fit the scaler/model or calibration threshold. Retrain existing artifacts to obtain these changes. A random holdout is a baseline, not a spatial/temporal generalization evaluation.

The dashboard's percentile slider recalculates a view-only threshold on the loaded scores; it does not retrain the model or overwrite its saved threshold. Existing SHAP images are labeled aggregate artifacts and must be checked against the data/model version before use.

The original dashboard is retained at `legacy_app.py` for reference and requires the full dependencies. The current application no longer uses top-k text retrieval to answer full-dataset numeric questions.

## Tests

```powershell
python -m unittest discover -s tests -v
python -m compileall -q geoai phase1_data_pipeline.py phase2_pytorch_anomaly.py
```

Tests cover complete-dataset aggregation, missing values, multi-record IDs, field mapping, evidence IDs, invalid query rejection, model-plan parsing with a fake response, imputation provenance, Streamlit interactions, and a small real autoencoder training/artifact check. Optional UI/ML tests skip when their dependencies are absent. CI checks the lightweight application and model workflow separately; no paid API calls or live scraping are required.
