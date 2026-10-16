import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


@unittest.skipUnless(importlib.util.find_spec("torch"), "Install full ML requirements for training checks")
class TrainingTests(unittest.TestCase):
    def test_training_artifacts_and_held_out_calibration(self):
        import numpy as np
        import pandas as pd
        import joblib
        from phase2_pytorch_anomaly import FEATURES, train_anomaly_detector
        rng = np.random.default_rng(42)
        frame = pd.DataFrame(rng.uniform(1, 5, (80, len(FEATURES))), columns=FEATURES)
        frame["Model_Training_Eligible"] = [True] * 70 + [False] * 10
        # An extreme unreviewed row must not influence scaler fitting or calibration.
        frame.loc[79, FEATURES] = 10000
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1]) as directory:
            directory = Path(directory)
            source = directory / "engineered.csv"
            output = directory / "scored.csv"
            artifacts = directory / "artifacts"
            frame.to_csv(source, index=False)
            train_anomaly_detector(str(source), str(output), artifacts, epochs=2, batch_size=16)
            metadata = json.loads((artifacts / "model_metadata.json").read_text())
            self.assertEqual(metadata["training_rows"] + metadata["validation_rows"], 70)
            self.assertEqual(metadata["threshold_source"], "held_out_validation_reconstruction_errors")
            self.assertTrue((artifacts / "urban_autoencoder.pt").exists())
            self.assertTrue((joblib.load(artifacts / "urban_scaler.joblib").mean_ < 5).all())
            scored = pd.read_csv(output)
            self.assertEqual(len(scored), 80)
            self.assertTrue(scored.iloc[79]["Anomaly_Review_Only"])


if __name__ == "__main__":
    unittest.main()
