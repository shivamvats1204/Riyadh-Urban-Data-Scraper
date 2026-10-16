import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from geoai.analysis import comparison_report, execute
from geoai.assistant import ModelError, Planner
from geoai.data import adapt
from phase1_data_pipeline import clean_and_prepare_data, NUMERIC_FEATURES, parse_regulatory_number

ROOT = Path(__file__).resolve().parents[1]


class AnalysisTests(unittest.TestCase):
    def setUp(self):
        self.frame = adapt(pd.read_csv(ROOT / "data/sample_parcels.csv", dtype=str), source_name="synthetic demo")

    def test_numeric_ranking_uses_all_records_and_missing_values(self):
        result = execute(self.frame, {"operation": "aggregate", "group_by": "zoningGroup", "metric": "maxBuildingCoefficient", "aggregation": "mean", "limit": 2})
        self.assertEqual(result["matched_records"], 8)
        self.assertEqual(result["rows"][0]["group"], "Commercial C")
        self.assertEqual(result["rows"][0]["value"], 5)
        self.assertEqual(result["rows"][0]["non_missing_values"], 1)
        self.assertEqual(result["rows"][1]["value"], 3.75)

    def test_filters_and_evidence(self):
        result = execute(self.frame, {"operation": "rows", "filters": [{"field": "maxBuildingCoefficient", "operator": "gte", "value": 4}]})
        self.assertEqual({row["parcel_id"] for row in result["rows"]}, {"DEMO-003", "DEMO-005"})
        self.assertTrue(all("source_record_id" in r for r in result["rows"]))

    def test_duplicate_ids_preserve_rules_and_count_locations(self):
        frame = pd.concat([self.frame, self.frame.iloc[[0]]], ignore_index=True)
        result = execute(frame, {"operation": "compare", "ids": ["DEMO-001"]})
        self.assertEqual(result["matched_records"], 2)
        self.assertEqual(result["matched_locations"], 1)
        result = execute(frame, {"operation": "aggregate", "aggregation": "unique_locations"})
        self.assertEqual(result["rows"][0]["value"], 8)

    def test_custom_mapping_and_raw_preservation(self):
        data = pd.DataFrame({"asset": ["001"], "far": ["60-75"], "x": ["٢٠٫٥"], "maxParcelCoverage": ["bad"]})
        frame = adapt(data, {"parcel_id": "asset", "maxBuildingCoefficient": "far", "lat": "x"})
        self.assertEqual(frame.iloc[0]["parcel_id"], "001")
        self.assertEqual(frame.iloc[0]["raw_maxBuildingCoefficient"], "60-75")
        self.assertTrue(pd.isna(frame.iloc[0]["maxBuildingCoefficient"]))
        self.assertAlmostEqual(frame.iloc[0]["lat"], 20.5)
        self.assertNotIn("maxParcelCoverage", frame)
        self.assertIn("original_maxParcelCoverage", frame)

    def test_invalid_queries_never_execute_code(self):
        for plan in [{"operation": "python", "query": "print(1)"}, {"operation": "rows", "limit": 100000},
                     {"operation": "aggregate", "metric": "description"}, {"sql": "DROP TABLE parcels"},
                     {"filters": [{"field": "lat", "operator": "gte", "value": "1 or 1=1"}]}]:
            with self.assertRaises(ValueError):
                execute(self.frame, plan)

    def test_empty_aggregate_does_not_invent_zero(self):
        result = execute(self.frame, {"operation": "aggregate", "metric": "maxBuildingCoefficient", "aggregation": "sum", "filters": [{"field": "parcel_id", "operator": "eq", "value": "DEMO-006"}]})
        self.assertIsNone(result["rows"][0]["value"])

    def test_llm_plan_validation_and_explanation(self):
        planner = Planner("test")
        plan = {"operation": "aggregate", "metric": "maxBuildingCoefficient", "aggregation": "mean", "group_by": "zoningGroup"}
        with patch.object(planner, "request", return_value={"tool_calls": [{"function": {"name": "query_dataset", "arguments": json.dumps(plan)}}]}):
            parsed = planner.plan("Which zone has the highest average FAR?", self.frame)
        result = execute(self.frame, parsed)
        with patch.object(planner, "request", return_value={"content": "Commercial C has the highest average in this synthetic dataset."}) as request:
            explanation = planner.explain("Explain", result)
            self.assertIn("Commercial C", explanation)
            self.assertIn("5.0", str(request.call_args))
        with patch.object(planner, "request", return_value={"content": "Trust me"}):
            with self.assertRaises(ModelError):
                planner.plan("Highest FAR?", self.frame)

    def test_report_contains_query_and_source_ids(self):
        report = comparison_report(execute(self.frame, {"operation": "compare", "ids": ["DEMO-001", "DEMO-003"]}))
        self.assertIn("synthetic demo", report)
        self.assertIn("source_record_id", report)

    def test_pipeline_tracks_imputation_and_raw_values(self):
        rows = [{**{f: 2 for f in NUMERIC_FEATURES}, "zoningGroup": "A", "api_status": "SUCCESS"},
                {**{f: None for f in NUMERIC_FEATURES}, "zoningGroup": "A", "api_status": "NOT_FOUND"}]
        with tempfile.TemporaryDirectory(dir=ROOT) as folder:
            path = Path(folder) / "input.csv"
            pd.DataFrame(rows).to_csv(path, index=False)
            cleaned = clean_and_prepare_data(str(path))
        self.assertTrue(cleaned.iloc[0]["Model_Training_Eligible"])
        self.assertFalse(cleaned.iloc[1]["Model_Training_Eligible"])
        self.assertTrue(cleaned.iloc[1]["maxBuildingCoefficient__imputed"])
        self.assertTrue(pd.isna(cleaned.iloc[1]["maxBuildingCoefficient__raw"]))
        self.assertNotEqual(cleaned.iloc[1]["Data_Quality_Flag"], "OK")
        self.assertEqual(parse_regulatory_number("60-75"), 75)
        self.assertEqual(parse_regulatory_number("٢٫٥"), 2.5)
        self.assertTrue(pd.isna(parse_regulatory_number("-5")))


if __name__ == "__main__":
    unittest.main()
