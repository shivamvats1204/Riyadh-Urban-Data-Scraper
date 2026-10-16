from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from PIL import Image

try:
    from langchain_classic.chains import create_retrieval_chain
    from langchain_classic.chains.combine_documents import create_stuff_documents_chain
    from langchain_community.embeddings import HuggingFaceEmbeddings
    from langchain_community.vectorstores import FAISS
    from langchain_core.prompts import PromptTemplate
    from langchain_groq import ChatGroq

    RAG_AVAILABLE = True
except ImportError:
    RAG_AVAILABLE = False


APP_TITLE = "RiyadhGeoAI: Urban Intelligence Platform"
SCORED_DATA = Path("riyadh_parcels_with_anomalies.csv")
ENGINEERED_DATA = Path("riyadh_parcels_engineered.csv")
SHAP_IMAGE = Path("shap_anomaly_explanation.png")

APP_COLUMNS = [
    "parcel_id",
    "parcel_objectid",
    "lon",
    "lat",
    "zoningGroup",
    "landuse",
    "description",
    "coloringDescription",
    "maxBuildingCoefficient",
    "maxParcelCoverage",
    "mainStreetsSetback",
    "secondaryStreetsSetback",
    "sideRearSetback",
    "Total_Setback",
    "Normalized_DPI",
    "Anomaly_Score_MSE",
    "Is_Anomaly",
    "api_status",
    "Data_Quality_Flag",
]


st.set_page_config(page_title=APP_TITLE, layout="wide", page_icon="AI")


@st.cache_data(show_spinner="Loading parcel intelligence layer...")
def load_data() -> tuple[pd.DataFrame, str]:
    source = SCORED_DATA if SCORED_DATA.exists() else ENGINEERED_DATA
    if not source.exists():
        st.error("No engineered dataset found. Run phase1_data_pipeline.py first.")
        st.stop()

    headers = pd.read_csv(source, nrows=0).columns
    usecols = [col for col in APP_COLUMNS if col in headers]
    df = pd.read_csv(source, usecols=usecols)

    if "Is_Anomaly" not in df.columns:
        df["Is_Anomaly"] = False
    if "Anomaly_Score_MSE" not in df.columns:
        df["Anomaly_Score_MSE"] = 0.0
    if "Data_Quality_Flag" not in df.columns:
        df["Data_Quality_Flag"] = "LEGACY_UNFLAGGED"
    if "Total_Setback" not in df.columns:
        setback_cols = ["mainStreetsSetback", "secondaryStreetsSetback", "sideRearSetback"]
        if all(col in df.columns for col in setback_cols):
            df["Total_Setback"] = df[setback_cols].sum(axis=1)

    df["zoningGroup"] = df.get("zoningGroup", pd.Series(dtype=str)).fillna("Unknown")
    df["landuse"] = df.get("landuse", pd.Series(dtype=str)).fillna("Unknown")
    df["Is_Anomaly"] = df["Is_Anomaly"].astype(bool)
    return df, source.name


@st.cache_resource(show_spinner="Indexing zoning rules...")
def build_vector_store(rule_frame: pd.DataFrame):
    unique_rules = (
        rule_frame[
            ["zoningGroup", "description", "coloringDescription", "maxBuildingCoefficient"]
        ]
        .dropna()
        .drop_duplicates()
    )
    documents = [
        (
            f"Zone Group: {row.zoningGroup}. Description: {row.description}. "
            f"Color detail: {row.coloringDescription}. "
            f"Max Building Coefficient FAR: {row.maxBuildingCoefficient}."
        )
        for row in unique_rules.itertuples(index=False)
    ]
    embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
    return FAISS.from_texts(documents, embeddings)


df, data_source = load_data()

st.title(APP_TITLE)
st.caption("Spatial data engineering, unsupervised anomaly detection, explainable AI, and zoning RAG.")

with st.sidebar:
    st.header("Filters")
    zoning_options = sorted(df["zoningGroup"].dropna().unique().tolist())
    selected_zones = st.multiselect("Zoning group", zoning_options, default=zoning_options[:8])
    anomaly_only = st.toggle("Show anomalies only", value=False)
    dpi_range = st.slider(
        "Development Potential Index",
        min_value=0.0,
        max_value=100.0,
        value=(0.0, 100.0),
        step=1.0,
    )
    sample_size = st.slider("Map sample size", 1000, 20000, 6000, step=1000)

filtered = df[df["zoningGroup"].isin(selected_zones)] if selected_zones else df.copy()
filtered = filtered[
    filtered["Normalized_DPI"].between(dpi_range[0], dpi_range[1], inclusive="both")
]
if anomaly_only:
    filtered = filtered[filtered["Is_Anomaly"]]

anomaly_count = int(df["Is_Anomaly"].sum())
malformed_count = int((df["Data_Quality_Flag"] != "OK").sum())
avg_dpi = float(df["Normalized_DPI"].mean())

metric_cols = st.columns(5)
metric_cols[0].metric("Parcels Scored", f"{len(df):,}")
metric_cols[1].metric("Anomalies", f"{anomaly_count:,}", f"{anomaly_count / max(len(df), 1):.2%}")
metric_cols[2].metric("Avg DPI", f"{avg_dpi:.2f}")
metric_cols[3].metric("Data Flags", f"{malformed_count:,}")
metric_cols[4].metric("Source", data_source)

overview_tab, map_tab, anomaly_tab, assistant_tab = st.tabs(
    ["Executive View", "Spatial Explorer", "Anomaly Lab", "Zoning Assistant"]
)

with overview_tab:
    left, right = st.columns([1.2, 1])
    with left:
        zone_summary = (
            df.groupby("zoningGroup", dropna=False)
            .agg(
                parcels=("parcel_id", "count"),
                avg_dpi=("Normalized_DPI", "mean"),
                anomalies=("Is_Anomaly", "sum"),
                avg_far=("maxBuildingCoefficient", "mean"),
            )
            .reset_index()
            .sort_values("avg_dpi", ascending=False)
            .head(15)
        )
        fig = px.bar(
            zone_summary,
            x="zoningGroup",
            y="avg_dpi",
            color="anomalies",
            hover_data=["parcels", "avg_far"],
            labels={"avg_dpi": "Average DPI", "zoningGroup": "Zoning Group"},
            title="Highest Development Potential by Zoning Group",
        )
        fig.update_layout(height=420, margin=dict(l=10, r=10, t=50, b=20))
        st.plotly_chart(fig, use_container_width=True)

    with right:
        status_summary = (
            df["Data_Quality_Flag"].value_counts(dropna=False).rename_axis("flag").reset_index(name="rows")
        )
        st.subheader("Data Quality")
        st.dataframe(status_summary, use_container_width=True, hide_index=True)
        st.subheader("Model Readiness")
        artifact_ready = Path("artifacts/urban_autoencoder.pt").exists()
        st.write("Model artifacts:", "Ready" if artifact_ready else "Run Phase 2")
        st.write("SHAP report:", "Ready" if SHAP_IMAGE.exists() else "Run Phase 3")

with map_tab:
    st.subheader("Parcel Potential and Anomaly Overlay")
    map_df = filtered.dropna(subset=["lat", "lon"])
    if map_df.empty:
        st.warning("No parcels match the current filters.")
    else:
        map_sample = map_df.sample(min(sample_size, len(map_df)), random_state=42)
        fig = px.scatter_mapbox(
            map_sample,
            lat="lat",
            lon="lon",
            color="Normalized_DPI",
            size="maxBuildingCoefficient",
            hover_name="parcel_id",
            hover_data={
                "zoningGroup": True,
                "landuse": True,
                "Is_Anomaly": True,
                "Anomaly_Score_MSE": ":.4f",
                "lat": False,
                "lon": False,
            },
            mapbox_style="carto-positron",
            zoom=10,
            height=650,
            color_continuous_scale="Turbo",
        )
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig, use_container_width=True)

with anomaly_tab:
    st.subheader("Explainable Anomaly Detection")
    left, right = st.columns([1, 1.4])
    with left:
        st.write(
            "The model uses an autoencoder trained on zoning and setback features. "
            "Parcels with the highest reconstruction error are flagged for review."
        )
        top_anomalies = (
            df[df["Is_Anomaly"]]
            .sort_values("Anomaly_Score_MSE", ascending=False)
            .head(25)
        )
        st.dataframe(
            top_anomalies[
                [
                    "parcel_id",
                    "zoningGroup",
                    "Normalized_DPI",
                    "maxBuildingCoefficient",
                    "maxParcelCoverage",
                    "Total_Setback",
                    "Anomaly_Score_MSE",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

    with right:
        if SHAP_IMAGE.exists():
            st.image(Image.open(SHAP_IMAGE), caption="SHAP drivers of autoencoder anomaly score")
        else:
            st.info("Run phase3_explainable_ai.py to generate the SHAP explanation image.")

with assistant_tab:
    st.subheader("Zoning Retrieval Assistant")
    st.write("Ask zoning questions grounded in the scraped parcel regulation records.")

    if not RAG_AVAILABLE:
        st.warning("Install the LangChain/Groq dependencies from requirements.txt to enable this tab.")
    else:
        groq_api_key = st.text_input("Groq API key", type="password")
        user_query = st.text_input(
            "Question",
            placeholder="Example: Which zoning groups allow the highest FAR?",
        )

        if groq_api_key and user_query:
            llm = ChatGroq(
                temperature=0,
                groq_api_key=groq_api_key,
                model_name="llama-3.1-8b-instant",
            )
            prompt = PromptTemplate.from_template(
                """You are an urban planning analyst for Riyadh.
Answer using only the retrieved zoning context. If the context is insufficient, say so.

Context:
{context}

Question: {input}

Answer:"""
            )
            vector_db = build_vector_store(df)
            document_chain = create_stuff_documents_chain(llm, prompt)
            retrieval_chain = create_retrieval_chain(
                vector_db.as_retriever(search_kwargs={"k": 4}),
                document_chain,
            )
            with st.spinner("Searching zoning records..."):
                response = retrieval_chain.invoke({"input": user_query})
                st.info(response["answer"])
        elif not groq_api_key:
            st.info("Enter a Groq API key to activate retrieval-augmented answers.")
