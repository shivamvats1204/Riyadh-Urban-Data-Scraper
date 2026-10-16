import hashlib
import json
from pathlib import Path

import pandas as pd
import streamlit as st

from .analysis import comparison_report, execute
from .assistant import ModelError, Planner
from .data import FIELDS, NUMERIC, adapt, profile

ROOT = Path(__file__).resolve().parents[1]


def show_result(result, key):
    cols = st.columns(3)
    cols[0].metric("Matching records", f"{result['matched_records']:,}")
    cols[1].metric("Unique locations", f"{result['matched_locations']:,}")
    cols[2].metric("Dataset records", f"{result['total_records']:,}")
    if result.get("rows"):
        st.dataframe(pd.DataFrame(result["rows"]), hide_index=True, width="stretch")
    elif result.get("profile"):
        st.json(result["profile"])
    else:
        st.info("No records match this query. Try broader criteria.")
    if result.get("truncated"):
        st.caption("The table is a limited result page. Matching counts cover the complete query scope.")
    with st.expander("Query, source evidence & assumptions"):
        st.json(result)
    st.download_button("Download evidence brief", comparison_report(result), "geoai-evidence.md", "text/markdown", key=f"brief-{key}")


def main():
    st.set_page_config(page_title="GeoAI · Location intelligence", page_icon="🌐", layout="wide")
    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env", override=False)
    except ImportError:
        pass
    st.title("GeoAI · Location intelligence")
    st.caption("Explore places. Ask better questions. Keep the evidence.")
    with st.sidebar:
        st.header("Your workspace")
        audience = st.selectbox("Perspective", ["General", "Urban planner", "Property researcher", "Data analyst"])
        options = ["Sample dataset", "Upload CSV"]
        local = next((ROOT / name for name in ["riyadh_parcels_with_anomalies.csv", "riyadh_parcels_engineered.csv"] if (ROOT / name).exists()), None)
        if local:
            options.insert(0, "Local Riyadh pipeline")
        selection = st.selectbox("Data source", options)
        upload = st.file_uploader("Choose CSV (up to 50 MB)", type=["csv"]) if selection == "Upload CSV" else None
        with st.expander("Advanced settings"):
            language = st.selectbox("Explanation language", ["English", "Arabic"])
            model = st.text_input("Model ID", "llama-3.3-70b-versatile", max_chars=120)
            limit = st.slider("Maximum result rows", 5, 100, 20, 5)
            percentile = st.slider("Anomaly review percentile", 90.0, 100.0, 99.0, 0.5)
            st.caption("A higher percentile flags fewer records. It changes the review overlay, not the trained model.")
            api_key = st.text_input("Groq API key (optional)", type="password", help="Or set GEOAI_API_KEY in .env. Never saved to the dataset.")
        st.caption("The query builder, maps, comparisons, and evidence exports work without an API key.")
    try:
        if selection == "Upload CSV":
            if not upload:
                st.info("Upload a CSV with a location ID. Coordinates, categories, and numeric planning fields are optional.")
                return
            if upload.size > 50_000_000:
                st.error("CSV limit is 50 MB.")
                return
            raw_bytes, source_name = upload.getvalue(), upload.name
            raw = pd.read_csv(upload, dtype=str, nrows=1_000_001)
        else:
            path = local if selection == "Local Riyadh pipeline" else ROOT / "data/sample_parcels.csv"
            raw_bytes, source_name = path.read_bytes(), path.name
            raw = pd.read_csv(path, dtype=str, nrows=1_000_001)
        dataset_key = hashlib.sha256(raw_bytes).hexdigest()[:12]
        if selection == "Sample dataset":
            st.info("DEMO: Every record is synthetic. Coordinates and constraints illustrate the workflow and are not real parcel evidence.")
        with st.expander("Dataset field mapping", expanded=selection == "Upload CSV"):
            st.write("Map your column names to the fields you want to analyze. Original columns are retained; numeric ranges remain unparsed rather than guessed.")
            mapping = {}
            columns = list(raw.columns)
            for index, (field, label) in enumerate(FIELDS.items()):
                choices = ["— not mapped —", *columns]
                default = choices.index(field) if field in columns else (1 if field == "parcel_id" and columns else 0)
                selected = st.selectbox(label, choices, index=default, key=f"map-{dataset_key}-{field}")
                if selected != choices[0]:
                    mapping[field] = selected
            st.caption("Coordinates must be WGS84 latitude/longitude in degrees. Projected coordinates need conversion before upload.")
        frame = adapt(raw, mapping, source_name)
    except (ValueError, OSError, pd.errors.ParserError, UnicodeError) as exc:
        st.error(f"Cannot open dataset: {exc}")
        return
    # Avoid stale evidence after switching source or field mappings.
    context_key = dataset_key + json.dumps(mapping, sort_keys=True)
    if st.session_state.get("context_key") != context_key:
        st.session_state.context_key = context_key
        st.session_state.pop("analysis", None)
        st.session_state.pop("explanation", None)
        st.session_state.pop("query_plan", None)
        st.session_state.pop("question", None)
    info = profile(frame)
    metric_cols = st.columns(4)
    metric_cols[0].metric("Source records", f"{info['records']:,}")
    metric_cols[1].metric("Unique locations", f"{info['unique_locations']:,}")
    metric_cols[2].metric("Mapped fields", len(mapping))
    numeric_fields = [c for c in FIELDS if c in NUMERIC and c in frame and frame[c].notna().any()]
    flags = frame["Data_Quality_Flag"].fillna("MISSING").ne("OK")
    metric_cols[3].metric("Records needing source review", int(flags.sum()))
    if info["multiple_records_per_location"]:
        st.warning("Some location IDs have multiple source records. Comparisons preserve those records; aggregates are record-weighted unless you explicitly count unique locations.")
    explore, compare, assistant, quality = st.tabs(["Explore & query", "Compare locations", "AI analyst", "Data & model quality"])
    with explore:
        st.subheader("Find what matters")
        operation = st.selectbox("Task", ["rows", "aggregate", "profile"], format_func=lambda x: {"rows": "Find locations", "aggregate": "Calculate & rank", "profile": "Understand the dataset"}[x])
        with st.form("query-builder"):
            categorical = [c for c in ("zoningGroup", "landuse") if c in frame]
            group, aggregation, metric = "None", "mean", "No numeric fields"
            if operation == "aggregate":
                group = st.selectbox("Group results by", ["None", *categorical], format_func=lambda x: FIELDS.get(x, x))
                aggregation = st.selectbox("Calculation", ["mean", "max", "min", "sum", "count", "unique_locations"], format_func=lambda x: {"mean": "Average", "max": "Highest", "min": "Lowest", "sum": "Total", "count": "Number of source records", "unique_locations": "Number of unique locations"}[x])
                available = numeric_fields or ["No numeric fields"]
                metric = st.selectbox("Numeric field", available, index=available.index("maxBuildingCoefficient") if "maxBuildingCoefficient" in available else 0, format_func=lambda x: FIELDS.get(x, x))
            with st.expander("Optional filter criteria"):
                filter_field = st.selectbox("Numeric filter", ["None", *numeric_fields], format_func=lambda x: FIELDS.get(x, x))
                filter_operator = st.selectbox("Comparison", ["gte", "lte", "gt", "lt"], format_func=lambda x: {"gte": "At least", "lte": "At most", "gt": "More than", "lt": "Less than"}[x])
                filter_value = st.number_input("Comparison value", value=0.0)
            categories = sorted(frame["zoningGroup"].dropna().unique().tolist()) if "zoningGroup" in frame else []
            zone = st.selectbox("Zone / category filter", ["All", *categories])
            run_query = st.form_submit_button("Run exact query", type="primary")
        if run_query:
            filters = []
            if filter_field != "None":
                filters.append({"field": filter_field, "operator": filter_operator, "value": filter_value})
            if zone != "All":
                filters.append({"field": "zoningGroup", "operator": "eq", "value": zone})
            plan = {"operation": operation, "filters": filters, "aggregation": aggregation, "limit": limit}
            if metric != "No numeric fields":
                plan["metric"] = metric
            if group != "None":
                plan["group_by"] = group
            try:
                st.session_state.builder_result = execute(frame, plan)
                st.session_state.builder_context = context_key
            except ValueError as exc:
                st.error(str(exc))
        if st.session_state.get("builder_context") == context_key:
            show_result(st.session_state.builder_result, "builder")
        if {"lat", "lon"}.issubset(frame):
            st.subheader("Spatial overview")
            mapped = frame.dropna(subset=["lat", "lon"])
            if not mapped.empty:
                st.map(mapped[["lat", "lon"]].sample(min(len(mapped), 5000), random_state=42), latitude="lat", longitude="lon")
                st.caption("Map displays up to 5,000 source records; calculations use the full loaded dataset.")
            else:
                st.info("No valid coordinates in this mapping.")
    with compare:
        st.subheader("Compare with evidence")
        id_text = st.text_input("Location IDs, separated by commas", placeholder="DEMO-001, DEMO-003")
        st.caption("Enter up to 10 IDs. All source records for those IDs remain visible.")
        if st.button("Build comparison brief"):
            try:
                ids = [i.strip() for i in id_text.split(",") if i.strip()]
                result = execute(frame, {"operation": "compare", "ids": ids, "limit": 100})
                st.session_state.comparison_result = result
                st.session_state.comparison_context = context_key
                missing = set(ids) - {row["parcel_id"] for row in result["rows"]}
                if missing:
                    st.warning("IDs absent from the returned page: " + ", ".join(sorted(missing)))
            except ValueError as exc:
                st.error(str(exc))
        if st.session_state.get("comparison_context") == context_key:
            show_result(st.session_state.comparison_result, "comparison")
    with assistant:
        st.subheader("Ask your data")
        st.write("Ask a question to compute exact results. Inspect the evidence and optionally generate an explanation.")
        question = st.text_area("Question", placeholder="Which zoning groups have the highest average FAR?", max_chars=4000)
        planner = Planner(api_key, model=model)
        if st.button("Analyze question", type="primary"):
            try:
                with st.spinner("Translating your question…"):
                    plan = planner.plan(question, frame)
                    result = execute(frame, plan)
                st.session_state.query_plan = json.dumps(plan, ensure_ascii=False, indent=2)
                st.session_state.question = question
                st.session_state.analysis = result
                st.session_state.pop("explanation", None)
            except (ValueError, ModelError) as exc:
                st.error(str(exc))
        # The editor also makes precise expert usage possible without a model/key.
        if "query_plan" not in st.session_state:
            st.session_state.query_plan = '{"operation":"profile"}'
        with st.expander("Advanced: inspect or edit the query"):
            st.caption("Edit the validated query JSON directly, including without a model key. No Python or SQL is executed.")
            query_text = st.text_area("Query plan", key="query_plan", height=180)
            if st.button("Compute evidence"):
                try:
                    st.session_state.analysis = execute(frame, json.loads(query_text))
                    st.session_state.pop("explanation", None)
                except (ValueError, TypeError) as exc:
                    st.error(str(exc))
        if "analysis" in st.session_state:
            show_result(st.session_state.analysis, "analyst")
            if st.button("Explain these results with AI"):
                try:
                    with st.spinner("Writing an evidence-based explanation…"):
                        st.session_state.explanation = planner.explain(st.session_state.get("question", question), st.session_state.analysis, audience, language)
                except (ModelError, ValueError) as exc:
                    st.error(str(exc))
            if "explanation" in st.session_state:
                st.caption("Generated explanation — inspect the exact results and source records above.")
                st.write(st.session_state.explanation)
    with quality:
        st.subheader("Know the limits of your evidence")
        st.dataframe(pd.DataFrame([{"field": f, **detail} for f, detail in info["fields"].items()]), hide_index=True, width="stretch")
        st.dataframe(frame["Data_Quality_Flag"].fillna("MISSING").value_counts().rename_axis("flag").reset_index(name="records"), hide_index=True)
        st.write("Numeric ranges and conditional rules remain available in raw columns. Missing values are not silently filled in this workspace.")
        if "Anomaly_Score_MSE" in frame and frame["Anomaly_Score_MSE"].notna().any():
            threshold = float(frame["Anomaly_Score_MSE"].quantile(percentile / 100))
            reviewed = frame[frame["Anomaly_Score_MSE"] >= threshold].sort_values("Anomaly_Score_MSE", ascending=False)
            st.metric("Records in review overlay", len(reviewed))
            st.caption(f"Threshold {threshold:.6g}, computed from this dataset at percentile {percentile:g}. Ties may flag more records. This is separate from the trained model's stored threshold.")
            st.dataframe(reviewed.head(100), hide_index=True, width="stretch")
        else:
            st.info("No anomaly scores loaded. Train and score the local dataset with Phase 2 to enable this view.")
        shap_image = ROOT / "shap_anomaly_explanation.png"
        if selection == "Local Riyadh pipeline" and shap_image.exists():
            st.image(str(shap_image), caption="Existing aggregate SHAP artifact. Verify it matches the current data/model before interpretation.")
        st.info("DPI is an explicit heuristic. Anomalies identify unusual records. Neither establishes property value, investment return, or a zoning violation.")
        safe_csv = frame.copy()
        for column in safe_csv.select_dtypes(include=["object", "string"]).columns:
            safe_csv[column] = safe_csv[column].map(lambda v: "'" + v if isinstance(v, str) and v.startswith(("=", "+", "-", "@", "\t", "\r")) else v)
        st.download_button("Export mapped data with provenance", safe_csv.to_csv(index=False), "geoai-mapped.csv", "text/csv")
