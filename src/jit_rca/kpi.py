from __future__ import annotations
import pandas as pd

def _ensure_datetime(df: pd.DataFrame) -> pd.DataFrame:
    for c in ["planned_time", "actual_time", "dispatch_time", "cutoff_time"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)
    return df

def _flag_on_time(row: pd.Series, tolerance_minutes: int = 0) -> bool:
    if pd.isna(row.get("planned_time")) or pd.isna(row.get("actual_time")):
        return False
    delta = (row["actual_time"] - row["planned_time"]).total_seconds() / 60.0
    return delta <= tolerance_minutes

def compute_kpi(df: pd.DataFrame, groupby: list[str] | None = None, tolerance_minutes: int = 0) -> dict:
    df = _ensure_datetime(df.copy())
    df["on_time"] = df.apply(lambda r: _flag_on_time(r, tolerance_minutes), axis=1)
    overall = {
        "orders": int(len(df)),
        "on_time": int(df["on_time"].sum()),
        "kpi_pct": round(100.0 * df["on_time"].mean(), 2) if len(df) else 0.0
    }
    detail = None
    if groupby:
        g = df.groupby(groupby)["on_time"].agg(["count","mean"]).reset_index()
        g["kpi_pct"] = (g["mean"] * 100).round(2)
        g = g.drop(columns=["mean"]).rename(columns={"count": "orders"})
        detail = g.to_dict(orient="records")
    return {"overall": overall, "detail": detail, "data": df}
