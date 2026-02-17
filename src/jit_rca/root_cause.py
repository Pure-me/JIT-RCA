# src/jit_rca/root_cause.py  (volledig, Python 3.9-compatibel)
from __future__ import annotations
import pandas as pd

__all__ = ["diagnose_root_causes"]

# Mapping van interne 'reason' labels naar oorzaakclusters
ROOT_CAUSE_MAP = {
    "late_dispatch": "Transport/Dispatch",
    "early_dispatch": "Transport/Dispatch",
    "last_minute_change": "Customer/Order Mgmt",
    "wms_tms_mismatch": "IT/Integration",
    "insufficient_stock": "Inventory",
    "picking_delay": "Operations/Picking",
    "unknown": "Unknown",
}

def _reason_bucket(reason: str) -> str:
    if not isinstance(reason, str) or not reason:
        return "Unknown"
    return ROOT_CAUSE_MAP.get(reason, "Other")

def diagnose_root_causes(df: pd.DataFrame, target_sla: float = 97.0) -> dict:
    """
    Verwacht een DataFrame met kolom 'on_time' (van compute_kpi).
    Retourneert dict met 'pareto' (per oorzaakcluster) en 'segments' (top segmentcombinaties).
    """
    if "on_time" not in df.columns:
        raise ValueError("diagnose_root_causes expects an 'on_time' column. Call compute_kpi first.")

    issues = df[~df["on_time"]].copy()
    if issues.empty:
        return {"pareto": [], "segments": [], "message": "No issues detected; KPI meets SLA."}

    if "reason" not in issues.columns:
        issues["reason"] = ""

    issues["reason_bucket"] = issues["reason"].apply(_reason_bucket)

    pareto = (
        issues.groupby("reason_bucket")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    pareto["share_pct"] = (pareto["count"] / pareto["count"].sum() * 100).round(2)

    segments = []
    keys = [k for k in ["customer", "site", "cnr_tour", "nm_short_unload"] if k in issues.columns]
    if keys:
        seg = (
            issues.groupby(keys)
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
        seg["share_pct"] = (seg["count"] / seg["count"].sum() * 100).round(2)
        segments = seg.to_dict(orient="records")

    return {"pareto": pareto.to_dict(orient="records"), "segments": segments}