import importlib.util
import unittest
from pathlib import Path


@unittest.skipUnless(importlib.util.find_spec("streamlit"), "Install requirements-app.txt")
class DashboardTests(unittest.TestCase):
    def test_demo_query_compare_and_ai_evidence(self):
        from streamlit.testing.v1 import AppTest
        app = AppTest.from_file(str(Path(__file__).resolve().parents[1] / "app.py"), default_timeout=30).run()
        self.assertEqual(len(app.exception), 0)
        app.button[0].click().run()
        self.assertEqual(len(app.exception), 0)
        # Check full-dataset result, then persist a comparison and compute a second result.
        self.assertEqual(app.session_state["builder_result"]["matched_records"], 8)
        ids = next(w for w in app.text_input if w.label.startswith("Location IDs"))
        ids.set_value("DEMO-001, DEMO-003")
        next(w for w in app.button if w.label == "Build comparison brief").click().run()
        self.assertEqual(len(app.exception), 0)
        next(w for w in app.button if w.label == "Compute evidence").click().run()
        self.assertEqual(len(app.exception), 0)
        self.assertEqual(app.session_state["analysis"]["profile"]["records"], 8)


if __name__ == "__main__":
    unittest.main()
