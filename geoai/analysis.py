import json
import math
import re

import numpy as np
import pandas as pd

from .data import CATEGORICAL, FIELDS, NUMERIC, profile


def records(frame):
    columns = [c for c in ["source_record_id", "source_dataset", *FIELDS] if c in frame]
    return json.loads(frame[columns].to_json(orient="records", force_ascii=False))


def execute(frame, plan):
    """Interpret a small query DSL. Never execute model-generated SQL/Python."""
    if not isinstance(plan, dict) or set(plan) - {"operation", "filters", "metric", "group_by", "aggregation", "sort_by", "descending", "limit", "ids", "query"}:
        raise ValueError("Unknown query fields")
    operation = plan.get("operation", "rows")
    if not isinstance(operation, str) or operation not in {"rows", "aggregate", "compare", "search", "profile"}:
        raise ValueError("Unsupported query operation")
    limit = plan.get("limit", 20)
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("Result limit must be 1–100")
    if type(plan.get("descending", True)) is not bool:
        raise ValueError("descending must be a boolean")
    filters = plan.get("filters", [])
    if not isinstance(filters, list) or len(filters) > 12:
        raise ValueError("At most 12 filters are supported")
    selected = frame.copy()
    for condition in filters:
        if not isinstance(condition, dict) or set(condition) != {"field", "operator", "value"}:
            raise ValueError("Each filter needs field, operator, and value")
        field, op, value = condition["field"], condition["operator"], condition["value"]
        if not isinstance(field, str) or not isinstance(op, str) or field not in FIELDS or field not in frame:
            raise ValueError(f"Unavailable filter field: {field}")
        if op in {"gte", "lte", "gt", "lt"}:
            if field not in NUMERIC or type(value) not in (int, float) or not math.isfinite(value):
                raise ValueError("Numeric comparisons need a finite number and numeric field")
            selected = selected[getattr(selected[field], {"gte": "ge", "lte": "le", "gt": "gt", "lt": "lt"}[op])(value)]
        elif op == "eq":
            if not isinstance(value, (str, float, int)) or isinstance(value, bool):
                raise ValueError("Equality requires text or a number")
            if field in NUMERIC and (type(value) not in (float, int) or not math.isfinite(value)):
                raise ValueError("Numeric equality requires a finite number")
            selected = selected[selected[field].eq(value) if field in NUMERIC else selected[field].astype("string").str.casefold().eq(str(value).casefold()).fillna(False)]
        elif op == "contains" and isinstance(value, str) and len(value) <= 200:
            selected = selected[selected[field].astype("string").str.contains(value, case=False, regex=False, na=False)]
        else:
            raise ValueError("Unsupported filter operator")
    if operation == "compare":
        ids = plan.get("ids", [])
        if not isinstance(ids, list) or not 1 <= len(ids) <= 10 or any(not isinstance(i, str) for i in ids):
            raise ValueError("Compare requires 1–10 string location IDs")
        selected = selected[selected["parcel_id"].isin(ids)]
    if operation == "search":
        query = plan.get("query", "")
        if not isinstance(query, str) or not 1 <= len(query) <= 1000:
            raise ValueError("Search needs 1–1000 characters")
        terms = set(re.findall(r"[^\W_]{2,}", query.casefold()))
        text_cols = [c for c in ("parcel_id", "description", "zoningGroup", "landuse") if c in selected]
        texts = selected[text_cols].fillna("").astype(str).agg(" ".join, axis=1).str.casefold()
        scores = texts.map(lambda text: sum(term in text for term in terms))
        selected = selected.loc[scores[scores > 0].sort_values(ascending=False).index]
    evidence = {
        "source_datasets": frame["source_dataset"].unique().tolist(), "total_records": len(frame),
        "matched_records": len(selected), "matched_locations": int(selected["parcel_id"].nunique()),
        "plan": plan, "notes": ["Counts distinguish source records from unique location IDs.",
            "Missing values are excluded from numeric aggregates. Raw values are retained.",
            "Source records are evidence from the loaded dataset, not verified current regulation.",
            "Anomaly scores indicate unusual patterns; DPI is a heuristic, not a valuation."],
    }
    if operation == "profile":
        return {**evidence, "profile": profile(selected), "rows": []}
    if operation == "aggregate":
        metric = plan.get("metric", "maxBuildingCoefficient")
        aggregation = plan.get("aggregation", "mean")
        group = plan.get("group_by") or None
        if not isinstance(aggregation, str) or aggregation not in {"mean", "min", "max", "sum", "count", "unique_locations"}:
            raise ValueError("Unsupported aggregation")
        if not isinstance(metric, str) or (group is not None and not isinstance(group, str)):
            raise ValueError("Metric and grouping fields must be strings")
        if aggregation not in {"count", "unique_locations"} and (metric not in NUMERIC or metric not in selected):
            raise ValueError("Choose an available numeric metric")
        if group and (group not in CATEGORICAL or group not in selected):
            raise ValueError("Choose an available category to group by")

        def aggregate(part):
            if aggregation == "count":
                return len(part)
            if aggregation == "unique_locations":
                return part["parcel_id"].nunique()
            values = part[metric].dropna()
            return getattr(values, aggregation)() if len(values) else np.nan

        parts = selected.groupby(group, dropna=False, sort=False) if group else [("All matching records", selected)]
        rows = [{"group": str(key), "value": aggregate(part), "records": len(part),
                 "non_missing_values": int(part[metric].notna().sum()) if metric in part else None} for key, part in parts]
        output = pd.DataFrame(rows, columns=["group", "value", "records", "non_missing_values"])
        output = output.sort_values("value", ascending=not plan.get("descending", True), na_position="last")
        return {**evidence, "aggregation": aggregation, "metric": metric if aggregation not in {"count", "unique_locations"} else None,
                "rows": json.loads(output.head(limit).to_json(orient="records")), "returned_rows": min(len(output), limit),
                "evidence_sample": records(selected.head(5))}
    sort_by = plan.get("sort_by")
    if sort_by:
        if not isinstance(sort_by, str) or sort_by not in FIELDS or sort_by not in selected:
            raise ValueError("Unavailable sort field")
        selected = selected.sort_values(sort_by, ascending=not plan.get("descending", True), na_position="last")
    return {**evidence, "rows": records(selected.head(limit)), "returned_rows": min(len(selected), limit),
            "truncated": len(selected) > limit}


def comparison_report(result, title="Location comparison"):
    """Reproducible report containing exact evidence, independent of an LLM."""
    return (f"# {title}\n\nSources: {', '.join(result['source_datasets'])}\n\n"
            f"Matching records: {result['matched_records']}; unique locations: {result['matched_locations']}\n\n"
            "## Query and evidence\n\n```json\n" + json.dumps(result, ensure_ascii=False, indent=2) + "\n```\n")
