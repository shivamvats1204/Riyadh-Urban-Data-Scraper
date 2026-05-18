# RiyadhGeoAI: Urban Intelligence Platform

RiyadhGeoAI is an end-to-end geospatial AI project for parcel-level urban planning analysis in Riyadh. It combines data scraping, regulatory feature engineering, unsupervised anomaly detection, explainable AI, interactive mapping, and retrieval-augmented zoning Q&A.

## Why This Project Matters

Urban planning teams need to identify parcels with unusual zoning constraints, high development potential, or inconsistent regulation records. This project turns raw parcel and building-rule data into a professional decision-support workflow:

- Clean and validate parcel regulation records.
- Engineer a Development Potential Index from FAR, parcel coverage, and setbacks.
- Train a PyTorch autoencoder to flag unusual zoning patterns.
- Explain anomaly drivers with SHAP.
- Explore parcels in a Streamlit dashboard with maps, filters, metrics, and zoning Q&A.

## Pipeline

1. Download parcel centroids:

```bash
python tile_downloader.py --output riyadh_parcels_centroids.csv
```

2. Enrich parcels with zoning/building rules:

```bash
python data_downloader.py --input riyadh_parcels_centroids.csv --output riyadh_parcels_full_data.csv
```

3. Clean data and engineer planning features:

```bash
python phase1_data_pipeline.py --input riyadh_parcels_full_data.csv --output riyadh_parcels_engineered.csv
```

4. Generate visual audit artifacts:

```bash
python phase1_5_viz.py --input riyadh_parcels_engineered.csv
```

5. Train and score the anomaly model:

```bash
python phase2_pytorch_anomaly.py --input riyadh_parcels_engineered.csv --output riyadh_parcels_with_anomalies.csv
```

6. Explain anomaly scores with SHAP:

```bash
python phase3_explainable_ai.py --input riyadh_parcels_with_anomalies.csv
```

7. Launch the dashboard:

```bash
streamlit run app.py
```

## Key Technical Fixes

- Regulatory number parsing now handles ranges such as `60-75` without turning them into invalid values like `6075`.
- Malformed `api_status` rows are flagged for data-quality review instead of silently polluting the model.
- The PyTorch model now saves reusable artifacts: model weights, scaler, metadata, anomaly threshold, and feature list.
- SHAP now explains the trained autoencoder's reconstruction-error score, not an untrained model.
- The dashboard reads the scored anomaly dataset when available and computes live metrics instead of using hardcoded values.

## Repository Notes

Large generated CSVs, images, local environments, and model artifacts are ignored by Git. For a portfolio or resume, keep a small sample dataset or publish the full data/model artifacts separately.
