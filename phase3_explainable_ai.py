import argparse
import json
import logging
from pathlib import Path

import joblib
import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import shap
import torch
import torch.nn as nn

from phase2_pytorch_anomaly import ARTIFACT_DIR, FEATURES, UrbanAutoencoder


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class ReconstructionErrorModel(nn.Module):
    """Wrap the autoencoder so SHAP explains anomaly score, not raw reconstruction."""

    def __init__(self, autoencoder: UrbanAutoencoder):
        super().__init__()
        self.autoencoder = autoencoder

    def forward(self, x):
        reconstructed = self.autoencoder(x)
        return torch.mean((x - reconstructed) ** 2, dim=1, keepdim=True)


def load_artifacts(artifact_dir: Path):
    model_path = artifact_dir / "urban_autoencoder.pt"
    scaler_path = artifact_dir / "urban_scaler.joblib"
    metadata_path = artifact_dir / "model_metadata.json"

    if not model_path.exists() or not scaler_path.exists():
        raise FileNotFoundError(
            "Model artifacts are missing. Run phase2_pytorch_anomaly.py before Phase 3."
        )

    checkpoint = torch.load(model_path, map_location="cpu")
    features = checkpoint.get("features", FEATURES)
    model = UrbanAutoencoder(input_dim=len(features))
    model.load_state_dict(checkpoint["model_state_dict"])
    model.eval()

    scaler = joblib.load(scaler_path)
    metadata = {}
    if metadata_path.exists():
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

    return model, scaler, features, metadata


def generate_shap_explanations(
    file_path: str,
    artifact_dir: Path = ARTIFACT_DIR,
    output_image: str = "shap_anomaly_explanation.png",
    max_anomalies: int = 300,
    background_size: int = 100,
    seed: int = 42,
) -> None:
    logging.info("Loading trained model artifacts from %s...", artifact_dir)
    autoencoder, scaler, features, metadata = load_artifacts(artifact_dir)

    df = pd.read_csv(file_path)
    missing = [feature for feature in features if feature not in df.columns]
    if missing:
        raise ValueError(f"Missing SHAP features in scored dataset: {missing}")
    if "Is_Anomaly" not in df.columns:
        raise ValueError("Scored dataset must include Is_Anomaly. Run Phase 2 first.")

    feature_frame = df[features].replace([np.inf, -np.inf], np.nan).fillna(0)
    scaled_features = scaler.transform(feature_frame).astype(np.float32)

    rng = np.random.default_rng(seed)
    anomaly_idx = df.index[df["Is_Anomaly"].astype(bool)].to_numpy()
    normal_idx = df.index[~df["Is_Anomaly"].astype(bool)].to_numpy()
    if len(anomaly_idx) == 0:
        raise ValueError("No anomalies found to explain.")

    anomaly_idx = rng.choice(anomaly_idx, size=min(max_anomalies, len(anomaly_idx)), replace=False)
    normal_idx = rng.choice(normal_idx, size=min(background_size, len(normal_idx)), replace=False)

    background = torch.from_numpy(scaled_features[normal_idx])
    anomaly_tensor = torch.from_numpy(scaled_features[anomaly_idx])

    scoring_model = ReconstructionErrorModel(autoencoder)
    scoring_model.eval()

    logging.info("Explaining %s anomalies against %s normal parcels...", len(anomaly_idx), len(normal_idx))
    explainer = shap.DeepExplainer(scoring_model, background)
    shap_values = explainer.shap_values(anomaly_tensor)

    if isinstance(shap_values, list):
        shap_array = shap_values[0]
    else:
        shap_array = shap_values
    shap_array = np.asarray(shap_array).squeeze()

    anomaly_features = pd.DataFrame(
        scaler.inverse_transform(anomaly_tensor.numpy()),
        columns=features,
    )

    plt.figure(figsize=(11, 6))
    title = "SHAP Feature Importance for Autoencoder Anomaly Score"
    if metadata.get("anomalies"):
        title += f" ({metadata['anomalies']:,} flagged parcels)"
    plt.title(title)
    shap.summary_plot(shap_array, anomaly_features, plot_type="bar", show=False)
    plt.tight_layout()
    plt.savefig(output_image, bbox_inches="tight", dpi=180)
    logging.info("Saved SHAP explanation to %s", output_image)


def main() -> None:
    parser = argparse.ArgumentParser(description="Explain trained parcel anomaly scores with SHAP.")
    parser.add_argument("--input", default="riyadh_parcels_with_anomalies.csv")
    parser.add_argument("--artifact-dir", default=str(ARTIFACT_DIR))
    parser.add_argument("--output-image", default="shap_anomaly_explanation.png")
    parser.add_argument("--max-anomalies", type=int, default=300)
    args = parser.parse_args()

    generate_shap_explanations(
        file_path=args.input,
        artifact_dir=Path(args.artifact_dir),
        output_image=args.output_image,
        max_anomalies=args.max_anomalies,
    )


if __name__ == "__main__":
    main()
