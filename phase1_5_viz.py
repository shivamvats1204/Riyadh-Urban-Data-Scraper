import argparse

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import seaborn as sns


def run_visual_audit(file_path: str, sample_size: int = 10000) -> None:
    df = pd.read_csv(file_path)
    sns.set_theme(style="whitegrid")

    if "Total_Setback" not in df.columns:
        df["Total_Setback"] = (
            df["mainStreetsSetback"] + df["secondaryStreetsSetback"] + df["sideRearSetback"]
        )

    sample = df.sample(min(sample_size, len(df)), random_state=42)

    plt.figure(figsize=(10, 6))
    sns.scatterplot(
        data=sample,
        x="Total_Setback",
        y="Normalized_DPI",
        hue="zoningGroup",
        alpha=0.45,
        linewidth=0,
    )
    plt.title("Impact of Regulatory Setbacks on Development Potential")
    plt.tight_layout()
    plt.savefig("setback_impact.png", dpi=180)

    top_zones = df["zoningGroup"].value_counts().head(12).index
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=df[df["zoningGroup"].isin(top_zones)], x="zoningGroup", y="Normalized_DPI")
    plt.xticks(rotation=35, ha="right")
    plt.title("Development Potential by Major Zoning Category")
    plt.tight_layout()
    plt.savefig("zoning_potential.png", dpi=180)

    fig = px.scatter_mapbox(
        sample.dropna(subset=["lat", "lon"]),
        lat="lat",
        lon="lon",
        color="Normalized_DPI",
        size="maxBuildingCoefficient",
        hover_name="landuse",
        hover_data=["zoningGroup", "maxParcelCoverage", "Total_Setback"],
        mapbox_style="carto-positron",
        zoom=10,
        title="Riyadh Urban Development Potential Heatmap",
        color_continuous_scale="Turbo",
    )
    fig.write_html("urban_heatmap.html")
    print("Visuals generated: setback_impact.png, zoning_potential.png, urban_heatmap.html")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate static and interactive visual audits.")
    parser.add_argument("--input", default="riyadh_parcels_engineered.csv")
    parser.add_argument("--sample-size", type=int, default=10000)
    args = parser.parse_args()
    run_visual_audit(args.input, args.sample_size)


if __name__ == "__main__":
    main()
