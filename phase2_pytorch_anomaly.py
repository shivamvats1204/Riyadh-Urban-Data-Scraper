import argparse
import json
import logging
import random
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from torch.utils.data import DataLoader, TensorDataset


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

FEATURES = [
    "maxBuildingCoefficient",
    "maxParcelCoverage",
    "mainStreetsSetback",
    "secondaryStreetsSetback",
    "sideRearSetback",
    "Normalized_DPI",
]

ARTIFACT_DIR = Path("artifacts")
MODEL_PATH = ARTIFACT_DIR / "urban_autoencoder.pt"
SCALER_PATH = ARTIFACT_DIR / "urban_scaler.joblib"
METADATA_PATH = ARTIFACT_DIR / "model_metadata.json"


def set_seed(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)


class UrbanAutoencoder(nn.Module):
    def __init__(self, input_dim: int):
        super().__init__()
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, 16),
            nn.ReLU(),
            nn.Linear(16, 8),
            nn.ReLU(),
            nn.Linear(8, 2),
        )
        self.decoder = nn.Sequential(
            nn.Linear(2, 8),
            nn.ReLU(),
            nn.Linear(8, 16),
            nn.ReLU(),
            nn.Linear(16, input_dim),
        )

    def forward(self, x):
        return self.decoder(self.encoder(x))


def validate_features(df: pd.DataFrame) -> pd.DataFrame:
    missing = [feature for feature in FEATURES if feature not in df.columns]
    if missing:
        raise ValueError(f"Missing required model features: {missing}")
    return df[FEATURES].replace([np.inf, -np.inf], np.nan).fillna(0)


def train_anomaly_detector(
    file_path: str,
    output_file: str = "riyadh_parcels_with_anomalies.csv",
    artifact_dir: Path = ARTIFACT_DIR,
    epochs: int = 150,
    batch_size: int = 8192,
    anomaly_percentile: float = 99.0,
    seed: int = 42,
) -> None:
    set_seed(seed)
    artifact_dir.mkdir(parents=True, exist_ok=True)

    logging.info("Loading engineered dataset from %s...", file_path)
    df = pd.read_csv(file_path)
    feature_frame = validate_features(df)

    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(feature_frame).astype(np.float32)
    train_data, val_data = train_test_split(scaled_data, test_size=0.15, random_state=seed)

    train_loader = DataLoader(
        TensorDataset(torch.from_numpy(train_data)),
        batch_size=batch_size,
        shuffle=True,
    )
    val_tensor = torch.from_numpy(val_data)

    model = UrbanAutoencoder(input_dim=len(FEATURES))
    criterion = nn.MSELoss()
    optimizer = optim.Adam(model.parameters(), lr=0.003, weight_decay=1e-5)

    best_loss = float("inf")
    best_state = None
    patience = 12
    patience_counter = 0

    logging.info("Training autoencoder on %s rows...", len(train_data))
    for epoch in range(1, epochs + 1):
        model.train()
        running_loss = 0.0
        for (batch,) in train_loader:
            optimizer.zero_grad()
            reconstructed = model(batch)
            loss = criterion(reconstructed, batch)
            loss.backward()
            optimizer.step()
            running_loss += loss.item() * len(batch)

        train_loss = running_loss / len(train_data)
        model.eval()
        with torch.no_grad():
            val_loss = criterion(model(val_tensor), val_tensor).item()

        if epoch == 1 or epoch % 10 == 0:
            logging.info("Epoch %03d | train_loss=%.6f | val_loss=%.6f", epoch, train_loss, val_loss)

        if val_loss < best_loss - 1e-5:
            best_loss = val_loss
            best_state = {key: value.detach().clone() for key, value in model.state_dict().items()}
            patience_counter = 0
        else:
            patience_counter += 1

        if patience_counter >= patience:
            logging.info("Early stopping at epoch %s with val_loss %.6f.", epoch, best_loss)
            break

    if best_state is not None:
        model.load_state_dict(best_state)

    logging.info("Scoring all parcels...")
    all_tensor = torch.from_numpy(scaled_data)
    model.eval()
    with torch.no_grad():
        reconstructed = model(all_tensor)
        mse_per_row = torch.mean((all_tensor - reconstructed) ** 2, dim=1).numpy()

    threshold = float(np.percentile(mse_per_row, anomaly_percentile))
    df["Anomaly_Score_MSE"] = mse_per_row
    df["Is_Anomaly"] = df["Anomaly_Score_MSE"] >= threshold

    df.to_csv(output_file, index=False, encoding="utf-8")

    checkpoint = {
        "model_state_dict": model.state_dict(),
        "input_dim": len(FEATURES),
        "features": FEATURES,
    }
    torch.save(checkpoint, artifact_dir / MODEL_PATH.name)
    joblib.dump(scaler, artifact_dir / SCALER_PATH.name)

    metadata = {
        "features": FEATURES,
        "rows_scored": int(len(df)),
        "anomaly_percentile": anomaly_percentile,
        "threshold": threshold,
        "anomalies": int(df["Is_Anomaly"].sum()),
        "best_validation_loss": best_loss,
        "seed": seed,
    }
    (artifact_dir / METADATA_PATH.name).write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    logging.info("Saved scored data to %s", output_file)
    logging.info("Saved model artifacts to %s", artifact_dir)
    logging.info("Flagged %s anomalies.", metadata["anomalies"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Train PyTorch autoencoder for parcel anomaly detection.")
    parser.add_argument("--input", default="riyadh_parcels_engineered.csv")
    parser.add_argument("--output", default="riyadh_parcels_with_anomalies.csv")
    parser.add_argument("--epochs", type=int, default=150)
    parser.add_argument("--batch-size", type=int, default=8192)
    parser.add_argument("--artifact-dir", default=str(ARTIFACT_DIR))
    args = parser.parse_args()

    train_anomaly_detector(
        file_path=args.input,
        output_file=args.output,
        artifact_dir=Path(args.artifact_dir),
        epochs=args.epochs,
        batch_size=args.batch_size,
    )


if __name__ == "__main__":
    main()
