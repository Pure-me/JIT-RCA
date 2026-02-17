# ================================================================
# jit_rca/analysis_views.py – JIT KPI & Root Cause Analyse (py3.9)
# ================================================================
from __future__ import annotations

from typing import Dict, Tuple

import numpy as np
import pandas as pd

# Mapping van klantnummers (cnr_cust) naar kanaal
CHANNEL_MAP: Dict[str, str] = {
    "Z41102": "express",
    "Z41103": "hyper",
    "Z41104": "partner",
    "Z41105": "Super / MKTI",
    "Z41108": "B2B",
}


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

def _parse_time_to_timedelta(series: pd.Series) -> pd.Series:
    """
    Converteer een kolom met tijden (bv. '06:00', '6:00') naar timedelta sinds middernacht.
    Verwacht korte tijdnotatie. Ongeldige waarden worden NaT.
    """
    s = series.astype(str).str.strip()
    s = s.replace({"NaT": "", "nan": "", "None": ""})
    # Zorg dat we altijd een hh:mm of hh:mm:ss hebben
    s = s.apply(lambda x: x if x.count(":") >= 1 else (x + ":00") if x else "")
    td = pd.to_timedelta(s, errors="coerce")
    return td


def _pct(n: float, d: float) -> float:
    """Percentage-helper met bescherming tegen delen door 0."""
    if d is None or d == 0 or pd.isna(d):
        return 0.0
    return float(round((n / d) * 100.0, 2))


# ------------------------------------------------------------
# Basisvoorbereiding: JIT-logica op order- en leveringsniveau
# ------------------------------------------------------------

def _prepare_window_df(df: pd.DataFrame, tolerance_minutes: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Voorbereiding van de dataset:
    - Filter op RFX Activity 4 & 5 (Delhaize & Carrefour)
    - Parsing van datum + tijdkolommen
    - JIT-vlaggen per order (Scenario 1 & 2)
    - Aggregatie naar leveringsniveau (per route / winkelpunt / tijdvenster)
    """
    required = [
        "date_dos",
        "cnr_tour",
        "cnr_cust",
        "RFX Activity",
        "nm_short_unload",
        "Win FROM",
        "Win UNTIL",
        "Planned",
        "Actual",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Dataset mist verplichte kolommen: {', '.join(missing)}")

    data = df.copy()

    # Filter op RFX Activity 4 & 5 (Delhaize & Carrefour)
    data["RFX Activity"] = data["RFX Activity"].astype(str).str.strip()
    data = data[data["RFX Activity"].isin(["4", "5"])]

    if data.empty:
        return data, data

    # Datum
    data["date_dos"] = pd.to_datetime(
        data["date_dos"], dayfirst=True, errors="coerce"
    ).dt.date

    # Stringkolommen
    for col in ["cnr_tour", "cnr_cust", "RFX Year", "RFX Preperation", "nm_short_unload"]:
        if col in data.columns:
            data[col] = data[col].astype(str).str.strip()

    # Tijdkolommen -> timedelta
    for col in ["Win FROM", "Win UNTIL", "Planned", "Actual"]:
        data[f"{col}_td"] = _parse_time_to_timedelta(data[col])

    base_date = pd.to_datetime(data["date_dos"])
    for col in ["Win FROM", "Win UNTIL", "Planned", "Actual"]:
        data[f"{col}_dt"] = base_date + data[f"{col}_td"]

    tol = pd.to_timedelta(tolerance_minutes, unit="m")

    # Scenario 1: Actual >= Win FROM AND Actual <= Win UNTIL (± tolerantie)
    data["order_jit_s1"] = (
        (data["Actual_dt"] >= (data["Win FROM_dt"] - tol))
        & (data["Actual_dt"] <= (data["Win UNTIL_dt"] + tol))
    )

    # Scenario 2: Actual <= Win UNTIL (+ tolerantie) – te vroeg = ook JIT
    data["order_jit_s2"] = data["Actual_dt"] <= (data["Win UNTIL_dt"] + tol)

    # Ongeldige tijdstempels → niet JIT
    valid_mask = data[["Win FROM_dt", "Win UNTIL_dt", "Actual_dt"]].notna().all(axis=1)
    data.loc[~valid_mask, ["order_jit_s1", "order_jit_s2"]] = False

    # Kanaal op basis van cnr_cust
    data["kanaal"] = data["cnr_cust"].map(CHANNEL_MAP).fillna("Overig")

    # Definieer levering (stop): per dag, route, klant, winkelpunt en levervenster
    delivery_cols = ["date_dos", "cnr_tour", "cnr_cust", "nm_short_unload", "Win FROM", "Win UNTIL"]

    deliveries = (
        data.groupby(delivery_cols, as_index=False)
        .agg(
            orders_total=("order_jit_s1", "size"),
            orders_jit_s1=("order_jit_s1", "sum"),
            orders_jit_s2=("order_jit_s2", "sum"),
            any_jit_s1=("order_jit_s1", "max"),
            any_jit_s2=("order_jit_s2", "max"),
            rfx_activity=("RFX Activity", "first"),
            kanaal=("kanaal", "first"),
        )
    )
    deliveries["delivery_jit_s1"] = deliveries["any_jit_s1"].astype(bool)
    deliveries["delivery_jit_s2"] = deliveries["any_jit_s2"].astype(bool)

    return data, deliveries


# ------------------------------------------------------------
# Wachttijd / root cause analyse
# ------------------------------------------------------------

def _waiting_time_rootcause(orders: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Root cause op basis van wachttijd:
    - gebruikt alleen late orders volgens Scenario 2 (order_jit_s2 == False)
    - DurationP = geplande wachttijd (min)
    - duration_A = effectieve wachttijd (min)
    """
    cols_needed = ["order_jit_s2", "nm_short_unload", "DurationP", "duration_A"]
    missing = [c for c in cols_needed if c not in orders.columns]
    if missing:
        # Geen wachttijdanalyse mogelijk
        empty_root = pd.DataFrame(columns=["rootcause_bucket", "aantal", "aandeel_%"])
        empty_store = pd.DataFrame(
            columns=[
                "nm_short_unload",
                "late_orders",
                "avg_DurationP_min",
                "avg_duration_A_min",
                "avg_extra_wachttijd_min",
            ]
        )
        return empty_root, empty_store

    late = orders[~orders["order_jit_s2"]].copy()
    if late.empty:
        empty_root = pd.DataFrame(columns=["rootcause_bucket", "aantal", "aandeel_%"])
        empty_store = pd.DataFrame(
            columns=[
                "nm_short_unload",
                "late_orders",
                "avg_DurationP_min",
                "avg_duration_A_min",
                "avg_extra_wachttijd_min",
            ]
        )
        return empty_root, empty_store

    # Zorg dat wachttijden numeriek zijn (minuten)
    late["DurationP"] = pd.to_numeric(late["DurationP"], errors="coerce")
    late["duration_A"] = pd.to_numeric(late["duration_A"], errors="coerce")

    late["extra_wachttijd_min"] = late["duration_A"] - late["DurationP"]

    # Buckets
    conditions = [
        late["extra_wachttijd_min"] > 30,
        (late["extra_wachttijd_min"] > 10) & (late["extra_wachttijd_min"] <= 30),
        (late["extra_wachttijd_min"] >= 0) & (late["extra_wachttijd_min"] <= 10),
        late["extra_wachttijd_min"] < 0,
    ]
    choices = [
        "Ernstige wachttijd >30 min",
        "Matige wachttijd 10–30 min",
        "Lichte wachttijd 0–10 min",
        "Geen extra wachttijd / vroeger weg",
    ]
    late["rootcause_bucket"] = np.select(
        conditions, choices, default="Onbekend / ontbrekende wachttijd"
    )

    root = (
        late.groupby("rootcause_bucket", as_index=False)
        .agg(aantal=("order_jit_s2", "size"))
        .sort_values("aantal", ascending=False)
    )
    total = root["aantal"].sum()
    root["aandeel_%"] = root["aantal"].apply(lambda x: _pct(x, total))

    # Per winkelpunt
    store = (
        late.groupby("nm_short_unload", as_index=False)
        .agg(
            late_orders=("order_jit_s2", "size"),
            avg_DurationP_min=("DurationP", "mean"),
            avg_duration_A_min=("duration_A", "mean"),
            avg_extra_wachttijd_min=("extra_wachttijd_min", "mean"),
        )
    )
    for c in ["avg_DurationP_min", "avg_duration_A_min", "avg_extra_wachttijd_min"]:
        store[c] = store[c].round(1)

    store = store.sort_values(
        ["late_orders", "avg_extra_wachttijd_min"], ascending=[False, False]
    ).head(15)

    return root, store


# ------------------------------------------------------------
# Hoofdfunctie: alle tabellen voor het rapport
# ------------------------------------------------------------

def jit_analysis_tables(df: pd.DataFrame, tolerance_minutes: int = 0) -> Dict[str, pd.DataFrame]:
    """
    Bouwt alle tabellen voor het JIT-rapport:

    - summary: totaal orders & leveringen, JIT in Scenario 1 & 2
    - daily_overview: per dag leveringen & orders met JIT%
    - by_rfx_activity: JIT% per RFX Activity (4/5)
    - by_channel: JIT% per kanaal (express / hyper / partner / Super-MKTI / B2B)
    - bottom_routes: bottom 10 routes (Scenario 2 – leveringen)
    - impact_stores: winkelpunten met meeste non-JIT leveringen (Scenario 2)
    - root_cause_buckets: verdeling van late orders over wachttijd-buckets
    - late_wait_by_store: wachttijd vs planning per winkelpunt (late orders)
    """
    orders, deliveries = _prepare_window_df(df, tolerance_minutes=tolerance_minutes)

    result: Dict[str, pd.DataFrame] = {}

    if orders.empty or deliveries.empty:
        # Geen data voor RFX 4/5 in de gekozen periode
        result["summary"] = pd.DataFrame()
        result["daily_overview"] = pd.DataFrame()
        result["by_rfx_activity"] = pd.DataFrame()
        result["by_channel"] = pd.DataFrame()
        result["bottom_routes"] = pd.DataFrame()
        result["impact_stores"] = pd.DataFrame()
        result["root_cause_buckets"] = pd.DataFrame()
        result["late_wait_by_store"] = pd.DataFrame()
        return result

    # --------------------------------------------------------
    # 1. Summary (één regel, voor KPI-kaarten)
    # --------------------------------------------------------
    total_orders = len(orders)
    jit_orders_s1 = int(orders["order_jit_s1"].sum())
    jit_orders_s2 = int(orders["order_jit_s2"].sum())

    total_deliveries = len(deliveries)
    jit_deliv_s1 = int(deliveries["delivery_jit_s1"].sum())
    jit_deliv_s2 = int(deliveries["delivery_jit_s2"].sum())

    summary = pd.DataFrame(
        [
            {
                "totaal_orders": total_orders,
                "JIT_orders_S1": jit_orders_s1,
                "JIT_orders_S2": jit_orders_s2,
                "JIT%_orders_S1": _pct(jit_orders_s1, total_orders),
                "JIT%_orders_S2": _pct(jit_orders_s2, total_orders),
                "totaal_leveringen": total_deliveries,
                "JIT_leveringen_S1": jit_deliv_s1,
                "JIT_leveringen_S2": jit_deliv_s2,
                "JIT%_leveringen_S1": _pct(jit_deliv_s1, total_deliveries),
                "JIT%_leveringen_S2": _pct(jit_deliv_s2, total_deliveries),
            }
        ]
    )
    result["summary"] = summary

    # --------------------------------------------------------
    # 2. Daily overview (per dag)
    # --------------------------------------------------------
    daily_deliv = (
        deliveries.groupby("date_dos", as_index=False)
        .agg(
            leveringen_totaal=("delivery_jit_s2", "size"),
            leveringen_jit_S1=("delivery_jit_s1", "sum"),
            leveringen_jit_S2=("delivery_jit_s2", "sum"),
        )
    )
    daily_orders = (
        orders.groupby("date_dos", as_index=False)
        .agg(
            orders_totaal=("order_jit_s2", "size"),
            orders_jit_S1=("order_jit_s1", "sum"),
            orders_jit_S2=("order_jit_s2", "sum"),
        )
    )

    daily = pd.merge(daily_deliv, daily_orders, on="date_dos", how="outer").fillna(0)
    daily["leveringen_totaal"] = daily["leveringen_totaal"].astype(int)
    daily["leveringen_jit_S1"] = daily["leveringen_jit_S1"].astype(int)
    daily["leveringen_jit_S2"] = daily["leveringen_jit_S2"].astype(int)
    daily["orders_totaal"] = daily["orders_totaal"].astype(int)
    daily["orders_jit_S1"] = daily["orders_jit_S1"].astype(int)
    daily["orders_jit_S2"] = daily["orders_jit_S2"].astype(int)

    daily["JIT%_lev_S1"] = daily.apply(
        lambda r: _pct(r["leveringen_jit_S1"], r["leveringen_totaal"]), axis=1
    )
    daily["JIT%_lev_S2"] = daily.apply(
        lambda r: _pct(r["leveringen_jit_S2"], r["leveringen_totaal"]), axis=1
    )
    daily["JIT%_ord_S1"] = daily.apply(
        lambda r: _pct(r["orders_jit_S1"], r["orders_totaal"]), axis=1
    )
    daily["JIT%_ord_S2"] = daily.apply(
        lambda r: _pct(r["orders_jit_S2"], r["orders_totaal"]), axis=1
    )

    daily = daily.sort_values("date_dos")
    result["daily_overview"] = daily

    # --------------------------------------------------------
    # 3. Per RFX Activity (4 vs 5)
    # --------------------------------------------------------
    by_rfx = (
        deliveries.groupby("rfx_activity", as_index=False)
        .agg(
            leveringen_totaal=("delivery_jit_s2", "size"),
            leveringen_jit_S1=("delivery_jit_s1", "sum"),
            leveringen_jit_S2=("delivery_jit_s2", "sum"),
        )
    )
    by_rfx["leveringen_totaal"] = by_rfx["leveringen_totaal"].astype(int)
    by_rfx["leveringen_jit_S1"] = by_rfx["leveringen_jit_S1"].astype(int)
    by_rfx["leveringen_jit_S2"] = by_rfx["leveringen_jit_S2"].astype(int)
    by_rfx["JIT%_lev_S1"] = by_rfx.apply(
        lambda r: _pct(r["leveringen_jit_S1"], r["leveringen_totaal"]), axis=1
    )
    by_rfx["JIT%_lev_S2"] = by_rfx.apply(
        lambda r: _pct(r["leveringen_jit_S2"], r["leveringen_totaal"]), axis=1
    )
    result["by_rfx_activity"] = by_rfx.rename(columns={"rfx_activity": "RFX Activity"})

    # --------------------------------------------------------
    # 4. Per kanaal (express / hyper / partner / Super-MKTI / B2B / Overig)
    # --------------------------------------------------------
    by_channel = (
        deliveries.groupby("kanaal", as_index=False)
        .agg(
            leveringen_totaal=("delivery_jit_s2", "size"),
            leveringen_jit_S1=("delivery_jit_s1", "sum"),
            leveringen_jit_S2=("delivery_jit_s2", "sum"),
        )
    )
    by_channel["leveringen_totaal"] = by_channel["leveringen_totaal"].astype(int)
    by_channel["leveringen_jit_S1"] = by_channel["leveringen_jit_S1"].astype(int)
    by_channel["leveringen_jit_S2"] = by_channel["leveringen_jit_S2"].astype(int)
    by_channel["JIT%_lev_S1"] = by_channel.apply(
        lambda r: _pct(r["leveringen_jit_S1"], r["leveringen_totaal"]), axis=1
    )
    by_channel["JIT%_lev_S2"] = by_channel.apply(
        lambda r: _pct(r["leveringen_jit_S2"], r["leveringen_totaal"]), axis=1
    )
    by_channel = by_channel.sort_values("leveringen_totaal", ascending=False)
    result["by_channel"] = by_channel

    # --------------------------------------------------------
    # 5. Bottom 10 routes – Scenario 2 (leveringen)
    # --------------------------------------------------------
    routes = (
        deliveries.groupby(["date_dos", "cnr_tour"], as_index=False)
        .agg(
            leveringen_totaal=("delivery_jit_s2", "size"),
            leveringen_jit_S2=("delivery_jit_s2", "sum"),
        )
    )
    routes["leveringen_totaal"] = routes["leveringen_totaal"].astype(int)
    routes["leveringen_jit_S2"] = routes["leveringen_jit_S2"].astype(int)
    routes["JIT%_lev_S2"] = routes.apply(
        lambda r: _pct(r["leveringen_jit_S2"], r["leveringen_totaal"]), axis=1
    )
    bottom_routes = routes.sort_values("JIT%_lev_S2", ascending=True).head(10)
    result["bottom_routes"] = bottom_routes

    # --------------------------------------------------------
    # 6. Winkelpunten met meeste non-JIT leveringen (Scenario 2)
    # --------------------------------------------------------
    deliveries["non_jit_S2"] = (~deliveries["delivery_jit_s2"]).astype(int)
    impact = (
        deliveries.groupby("nm_short_unload", as_index=False)
        .agg(
            leveringen_totaal=("delivery_jit_s2", "size"),
            non_jit_leveringen=("non_jit_S2", "sum"),
        )
    )
    impact["leveringen_totaal"] = impact["leveringen_totaal"].astype(int)
    impact["non_jit_leveringen"] = impact["non_jit_leveringen"].astype(int)
    impact["non_jit_%"] = impact.apply(
        lambda r: _pct(r["non_jit_leveringen"], r["leveringen_totaal"]), axis=1
    )
    impact = impact.sort_values(
        ["non_jit_leveringen", "non_jit_%"], ascending=[False, False]
    ).head(15)
    result["impact_stores"] = impact.rename(columns={"nm_short_unload": "winkelpunt"})

    # --------------------------------------------------------
    # 7. Wachttijd / root cause buckets & per winkelpunt
    # --------------------------------------------------------
    root_buckets, wait_by_store = _waiting_time_rootcause(orders)
    result["root_cause_buckets"] = root_buckets
    result["late_wait_by_store"] = wait_by_store

    return result
# ------------------------------------------------------------
# Route detail – per order & per levering voor één route
# ------------------------------------------------------------

def jit_route_detail(
    df: pd.DataFrame,
    date_dos: str,
    cnr_tour: str,
    tolerance_minutes: int = 0,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Detailanalyse voor één route op één dag.

    - gebruikt dezelfde JIT-logica als jit_analysis_tables (Scenario 1 & 2)
    - filtert op date_dos + cnr_tour
    - geeft terug:
        * orders_detail: per order met JIT-vlaggen
        * deliveries_detail: per levering (winkelpunt / venster) met JIT-vlaggen
    """
    # Hergebruik exact dezelfde voorbereiding
    orders_all, deliveries_all = _prepare_window_df(df, tolerance_minutes=tolerance_minutes)

    if orders_all.empty or deliveries_all.empty:
        return orders_all.copy(), deliveries_all.copy()

    target_date = pd.to_datetime(date_dos).date()
    route = str(cnr_tour)

    # Filter op datum + route
    orders = orders_all[
        (orders_all["date_dos"] == target_date)
        & (orders_all["cnr_tour"] == route)
    ].copy()

    deliveries = deliveries_all[
        (deliveries_all["date_dos"] == target_date)
        & (deliveries_all["cnr_tour"] == route)
    ].copy()

    # Mooie subset van kolommen (orders)
    order_cols = [
        "date_dos",
        "cnr_tour",
        "cnr_cust",
        "RFX Activity",
        "kanaal",
        "nm_short_unload",
        "Win FROM",
        "Win UNTIL",
        "Planned",
        "Actual",
        "order_jit_s1",
        "order_jit_s2",
    ]
    for c in order_cols:
        if c not in orders.columns:
            orders[c] = np.nan

    orders = orders[order_cols].sort_values(
        ["date_dos", "cnr_tour", "nm_short_unload", "Actual"]
    )

    # Mooie subset van kolommen (leveringen)
    deliv_cols = [
        "date_dos",
        "cnr_tour",
        "cnr_cust",
        "kanaal",
        "nm_short_unload",
        "Win FROM",
        "Win UNTIL",
        "orders_total",
        "orders_jit_s1",
        "orders_jit_s2",
        "delivery_jit_s1",
        "delivery_jit_s2",
    ]
    for c in deliv_cols:
        if c not in deliveries.columns:
            deliveries[c] = np.nan

    deliveries = deliveries[deliv_cols].sort_values(
        ["date_dos", "cnr_tour", "nm_short_unload", "Win FROM"]
    )

    return orders, deliveries