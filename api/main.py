from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple
import sqlite3
import io

import pandas as pd
from fastapi import FastAPI, UploadFile, File, Query
from fastapi.responses import HTMLResponse

app = FastAPI(title="JIT KPI RCA")

# ------------------------------------------------------------
# Pad naar SQLite database
# ------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = ROOT_DIR / "jit.sqlite"

# ------------------------------------------------------------
# Helper: basis HTML layout met navigatie
# ------------------------------------------------------------
def _layout(title: str, body_html: str) -> HTMLResponse:
    html = f"""
    <!doctype html>
    <html>
    <head>
        <meta charset="utf-8"/>
        <title>{title}</title>
        <style>
            :root {{
              --bg:#020617;
              --bg-card:#020617;
              --border:#1f2937;
              --text:#e5e7eb;
              --muted:#94a3b8;
              --link:#60a5fa;
              --chip:#0f172a;
            }}
            * {{ box-sizing:border-box; }}
            body {{
              margin:0;
              font-family:-apple-system, system-ui, BlinkMacSystemFont, "Segoe UI", sans-serif;
              background:radial-gradient(circle at top, #0b1120, #020617 55%);
              color:var(--text);
            }}
            a {{ color:var(--link); text-decoration:none; }}
            .wrap {{ max-width:1400px; margin:0 auto; padding:24px 24px 40px; }}
            .nav {{
              display:flex; align-items:center; justify-content:space-between;
              padding:12px 20px; border-radius:16px;
              background:rgba(15,23,42,0.9); border:1px solid var(--border);
              margin-bottom:20px; backdrop-filter:blur(14px);
            }}
            .nav-title {{ font-size:18px; font-weight:600; }}
            .nav-links a {{
              margin-left:14px; font-size:14px; color:var(--muted);
              padding:6px 10px; border-radius:999px; background:rgba(15,23,42,0.7);
            }}
            .nav-links a:hover {{ color:var(--text); background:#111827; }}
            h1 {{ font-size:26px; margin:0 0 4px; }}
            h2 {{ font-size:20px; margin:18px 0 8px; }}
            h3 {{ font-size:16px; margin:16px 0 6px; }}
            .sub {{ color:var(--muted); font-size:14px; margin:4px 0 12px; }}
            .grid-cards {{
              display:grid; grid-template-columns:repeat(auto-fit,minmax(260px,1fr));
              gap:14px; margin:6px 0 18px;
            }}
            .card {{
              background:var(--bg-card); border-radius:18px;
              border:1px solid var(--border);
              padding:14px 16px;
            }}
            .card h3 {{ margin-top:0; margin-bottom:6px; font-size:16px; }}
            .card p {{ margin:0; font-size:13px; color:var(--muted); }}
            .btn {{
              display:inline-block; padding:8px 14px; border-radius:999px;
              border:1px solid var(--border); background:#020617;
              color:var(--text); font-size:14px;
            }}
            .btn-primary {{
              background:linear-gradient(135deg,#22c55e,#16a34a);
              border:none; color:#022c22;
            }}
            .btn:hover {{ filter:brightness(1.1); }}
            form.inline {{ display:flex; flex-wrap:wrap; gap:8px; align-items:center; }}
            label.small {{ font-size:13px; color:var(--muted); }}
            input, select {{
              background:#020617; border-radius:999px; border:1px solid var(--border);
              color:var(--text); font-size:13px; padding:6px 10px;
            }}
            input[type="text"] {{ width:140px; }}
            input[type="date"] {{ width:155px; }}
            input[type="file"] {{ border-radius:999px; padding:6px 10px; }}
            .table-wrapper {{
              border-radius:16px; border:1px solid var(--border);
              background:rgba(15,23,42,0.9); overflow:auto;
            }}
            table {{
              width:100%; border-collapse:collapse; font-size:13px;
              white-space:nowrap;
            }}
            thead tr {{ background:#020617; }}
            th, td {{
              padding:8px 10px; border-bottom:1px solid #111827;
              text-align:left;
            }}
            tbody tr:nth-child(even) td {{ background:#020617; }}
            tbody tr:hover td {{ background:#030712; }}
            .mono {{ font-variant-numeric:tabular-nums; }}
            .topbar {{
              display:flex; justify-content:space-between; align-items:flex-end;
              gap:12px; margin-bottom:12px;
            }}
            .copy-btn {{
              border:none; background:#020617; border-radius:999px;
              border:1px solid var(--border); padding:6px 12px;
              font-size:13px; color:var(--muted); cursor:pointer;
            }}
            .copy-btn:hover {{ color:var(--text); }}
            .jit-ok td {{ background:#022c22; }}
            .jit-late td {{ background:#3f2a0a; }}
            .jit-root td {{ background:#3b0f0f; }}

            .route-good td {{ background:#022c22; }}
            .route-mid td {{ background:#3f2a0a; }}
            .route-bad td {{ background:#3b0f0f; }}

            @media (max-width:800px) {{
              .nav {{ flex-direction:column; align-items:flex-start; gap:8px; }}
              .topbar {{ flex-direction:column; align-items:flex-start; }}
            }}
        </style>
        <script>
          function copyTable(id) {{
              const table = document.getElementById(id);
              if (!table) return;
              let text = "";
              const rows = table.querySelectorAll("tr");
              rows.forEach((row, idx) => {{
                  const cells = row.querySelectorAll("th,td");
                  const rowText = Array.from(cells).map(c => c.innerText.trim()).join("\\t");
                  text += rowText + (idx < rows.length-1 ? "\\n" : "");
              }});
              navigator.clipboard.writeText(text);
          }}
        </script>
    </head>
    <body>
      <div class="wrap">
        <div class="nav">
          <div class="nav-title">JIT KPI RCA Dashboard</div>
          <div class="nav-links">
            <a href="/">🏠 Home</a>
            <a href="/upload">📁 Upload</a>
            <a href="/dataset_html">📊 Dataset</a>
            <a href="/routes_html">🚛 Routes &amp; JIT</a>
            <a href="/waits_html">⏱ Wachttijden</a>
            <a href="/rca_delay_drivers_html">🧩 RCA</a>
            <a href="/jit_outside_daily_html">⛔ Outside JIT</a>
            <a href="/outside_jit_daily_html">📦 Buckets Outside</a>
            <a href="/transport_manager_html">🧭 Transport</a>
          </div>
        </div>
        {body_html}
      </div>
    </body>
    </html>
    """
    return HTMLResponse(html)

# ------------------------------------------------------------
# Database helpers
# ------------------------------------------------------------
def ensure_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                date_dos TEXT,
                cnr_tour TEXT,
                cnr_cust TEXT,
                "RFX Activity" TEXT,
                "RFX Year" TEXT,
                "RFX Preperation" TEXT,
                nm_short_unload TEXT,
                "Win FROM" TEXT,
                "Win UNTIL" TEXT,
                Planned TEXT,
                P_Depart TEXT,
                DurationP REAL,
                Actual TEXT,
                A_Depart TEXT,
                DurationA REAL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()

def load_orders(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    rfx_activity: Optional[str] = None,
    cnr_tour: Optional[str] = None,
) -> pd.DataFrame:
    ensure_db()
    if not DB_PATH.exists():
        return pd.DataFrame()

    conn = sqlite3.connect(DB_PATH)
    try:
        where = []
        params: list = []
        if date_from:
            where.append("date_dos >= ?")
            params.append(date_from)
        if date_to:
            where.append("date_dos <= ?")
            params.append(date_to)
        if rfx_activity:
            where.append('"RFX Activity" = ?')
            params.append(rfx_activity)
        if cnr_tour:
            where.append("cnr_tour = ?")
            params.append(str(cnr_tour))

        where_sql = " WHERE " + " AND ".join(where) if where else ""
        sql = "SELECT * FROM orders" + where_sql
        df = pd.read_sql_query(sql, conn, params=params)
    finally:
        conn.close()

    if df.empty:
        return df

    if "cnr_tour" in df.columns:
        df["cnr_tour"] = df["cnr_tour"].astype(str).str.replace(r"\.0$", "", regex=True)

    for col in ["RFX Activity", "RFX Year", "RFX Preperation", "cnr_cust"]:
        if col in df.columns:
            df[col] = df[col].astype(str)

    return df

# ------------------------------------------------------------
# Tijdhelpers & JIT berekening
# ------------------------------------------------------------
def _combine_datetime(df: pd.DataFrame, date_col: str, time_col: str) -> pd.Series:
    """
    Combineer datum + tijd tot datetime.
    Ondersteunt HH:MM én HH:MM:SS. Verwacht date_dos als YYYY-MM-DD.
    """
    date_str = df[date_col].astype(str)
    time_str = df[time_col].astype(str)

    s = date_str + " " + time_str
    dt = pd.to_datetime(s, format="%Y-%m-%d %H:%M", errors="coerce")

    mask = dt.isna() & time_str.notna()
    if mask.any():
        s2 = date_str[mask] + " " + time_str[mask]
        dt2 = pd.to_datetime(s2, format="%Y-%m-%d %H:%M:%S", errors="coerce")
        dt.loc[mask] = dt2

    return dt

def _fmt_hhmm(x: object) -> str:
    """Toon altijd short time HH:MM (als het op tijd lijkt)."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x).strip()
    if s.lower() in ("nan", "none", ""):
        return ""
    if len(s) >= 5 and s[2] == ":":
        return s[:5]
    t = pd.to_datetime(s, errors="coerce")
    if pd.isna(t):
        return s
    return t.strftime("%H:%M")

def compute_jit(route_orders: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Scenario 1 (S1): Actual >= Win FROM én Actual <= Win UNTIL.
    Scenario 2 (S2): Actual <= Win UNTIL (te vroeg = ook JIT).
    Eén order binnen venster => volledige levering JIT.
    """
    df = route_orders.copy()

    df["win_from_dt"] = _combine_datetime(df, "date_dos", "Win FROM")
    df["win_until_dt"] = _combine_datetime(df, "date_dos", "Win UNTIL")
    df["actual_dt"] = _combine_datetime(df, "date_dos", "Actual")
    df["planned_dt"] = _combine_datetime(df, "date_dos", "Planned")

    s1 = (df["actual_dt"] >= df["win_from_dt"]) & (df["actual_dt"] <= df["win_until_dt"])
    s2 = df["actual_dt"] <= df["win_until_dt"]

    df["jit_s1_order"] = s1.fillna(False)
    df["jit_s2_order"] = s2.fillna(False)

    grp_cols = ["date_dos", "cnr_tour", "nm_short_unload"]
    deliveries = (
        df.groupby(grp_cols, as_index=False)
        .agg(
            orders=("cnr_cust", "count"),
            win_from=("Win FROM", "first"),
            win_until=("Win UNTIL", "first"),
            first_planned=("Planned", "first"),
            first_actual=("Actual", "first"),
            jit_s1_delivery=("jit_s1_order", "any"),
            jit_s2_delivery=("jit_s2_order", "any"),
            first_planned_dt=("planned_dt", "min"),
        )
        .sort_values(["first_planned_dt", "nm_short_unload"])
    )
    return df, deliveries

# ------------------------------------------------------------
# HOME
# ------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
def home():
    body = """
    <h1>JIT KPI RCA – Dashboard</h1>
    <p class="sub">Hoofdpagina met navigatie naar upload, dataset, routes, wachttijden, outside-JIT, RCA en transport analyse.</p>

    <div class="grid-cards">
      <div class="card">
        <h3>1. Dataset uploaden</h3>
        <p>Laad de OTIF Excel in, de orders-tabel wordt leeggemaakt en opnieuw opgebouwd.</p>
        <p style="margin-top:10px"><a href="/upload" class="btn btn-primary">📁 Upload dataset</a></p>
      </div>

      <div class="card">
        <h3>2. Dataset bekijken</h3>
        <p>Filter en controleer de ruwe gegevens (incl. datum, route, RFX Activity, tijden).</p>
        <p style="margin-top:10px"><a href="/dataset_html" class="btn">📊 Open dataset viewer</a></p>
      </div>

      <div class="card">
        <h3>3. Routes &amp; JIT analyse</h3>
        <p>Overzicht per route met JIT Scenario 1 &amp; 2, filter op klant &amp; route-nummer.</p>
        <p style="margin-top:10px"><a href="/routes_html" class="btn">🚛 Bekijk routes</a></p>
      </div>

      <div class="card">
        <h3>4. Wachttijden analyse</h3>
        <p>Overzicht wachttijden per klant, met drill-down naar leverpunten en orders.</p>
        <p style="margin-top:10px"><a href="/waits_html" class="btn">⏱ Bekijk wachttijden</a></p>
      </div>

      <div class="card">
        <h3>5. RCA – Delay drivers</h3>
        <p>Late departure vs transit (proxy), buckets minuten te laat, en drill-down per klant/dag.</p>
        <p style="margin-top:10px"><a href="/rca_delay_drivers_html" class="btn">🧩 Open RCA</a></p>
      </div>

      <div class="card">
        <h3>6. Outside JIT (S2) per dag</h3>
        <p>Aantal leverpunten buiten levervenster (Actual &gt; Win UNTIL) per dag + detail.</p>
        <p style="margin-top:10px"><a href="/jit_outside_daily_html" class="btn">⛔ Outside JIT</a></p>
      </div>

      <div class="card">
        <h3>7. Buckets Outside JIT</h3>
        <p>0–15 / 15–30 / 30–45 / 45–60 / 60+ + % en cumulatieve impact op JIT%.</p>
        <p style="margin-top:10px"><a href="/outside_jit_daily_html" class="btn">📦 Buckets</a></p>
      </div>

      <div class="card">
        <h3>8. Transport manager analyse</h3>
        <p>Volgorde drops vs planning, vertrek reëel vs gepland, en planned+wait vs reële levertijd+wachttijd.</p>
        <p style="margin-top:10px"><a href="/transport_manager_html" class="btn">🧭 Open transport analyse</a></p>
      </div>
    </div>
    """
    return _layout("Dashboard", body)

# ------------------------------------------------------------
# UPLOAD
# ------------------------------------------------------------
@app.get("/upload", response_class=HTMLResponse)
def upload_form():
    body = """
    <h1>Dataset uploaden</h1>
    <p class="sub">Laad hier de OTIF Excel file op. De bestaande tabel <code>orders</code> wordt vervangen.</p>
    <form action="/upload" method="post" enctype="multipart/form-data">
      <label class="small">Excel bestand (.xlsx)</label><br/>
      <input type="file" name="file" accept=".xlsx" required />
      <br/><br/>
      <button type="submit" class="btn btn-primary">Upload &amp; verwerk</button>
    </form>
    """
    return _layout("Upload", body)

@app.post("/upload", response_class=HTMLResponse)
async def upload(file: UploadFile = File(...)):
    ensure_db()
    raw = await file.read()
    excel = io.BytesIO(raw)

    df = pd.read_excel(excel, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]

    if "date_dos" in df.columns:
        df["date_dos"] = pd.to_datetime(df["date_dos"], errors="coerce").dt.strftime("%Y-%m-%d")

    for col in ["cnr_tour", "cnr_cust", "RFX Activity", "RFX Year", "RFX Preperation"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Short time HH:MM
    time_cols = ["Win FROM", "Win UNTIL", "Planned", "Actual", "P_Depart", "A_Depart"]
    for col in time_cols:
        if col in df.columns:
            t = pd.to_datetime(df[col], errors="coerce").dt.strftime("%H:%M")
            df[col] = t

    conn = sqlite3.connect(DB_PATH)
    try:
        df.to_sql("orders", conn, if_exists="replace", index=False)
        conn.commit()
    finally:
        conn.close()

    body = f"""
    <h1>Upload resultaat</h1>
    <p class="sub">
      Bestand <strong>{file.filename}</strong> werd verwerkt.
      Aantal rijen in tabel <code>orders</code>: {len(df)}.
    </p>
    <p>
      <a href="/dataset_html" class="btn">📊 Bekijk dataset</a>
      &nbsp;
      <a href="/routes_html" class="btn">🚛 Naar routes analyse</a>
      &nbsp;
      <a href="/waits_html" class="btn">⏱ Naar wachttijden</a>
      &nbsp;
      <a href="/transport_manager_html" class="btn">🧭 Transport</a>
    </p>
    """
    return _layout("Upload OK", body)

# ------------------------------------------------------------
# DATASET VIEWER
# ------------------------------------------------------------
@app.get("/dataset_html", response_class=HTMLResponse)
def dataset_html(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    rfx_activity: Optional[str] = Query(None),
    cnr_tour: Optional[str] = Query(None),
):
    df = load_orders(date_from, date_to, rfx_activity, cnr_tour)

    max_rows = 500
    if df.empty:
        table_html = "<p class='sub'>Geen gegevens beschikbaar (controleer filters of upload eerst een dataset).</p>"
    else:
        df_view = df.head(max_rows) if len(df) > max_rows else df
        headers = "".join(f"<th>{c}</th>" for c in df_view.columns)
        rows = ""
        for _, row in df_view.iterrows():
            cells = "".join(f"<td>{row[c]}</td>" for c in df_view.columns)
            rows += f"<tr>{cells}</tr>"
        table_html = f"""
        <div class="topbar">
          <div class="sub">Aantal rijen getoond: {len(df_view)} (beperkt tot {max_rows}).</div>
          <button class="copy-btn" onclick="copyTable('tblDataset')">📋 Kopieer tabel</button>
        </div>
        <div class="table-wrapper">
          <table id="tblDataset">
            <thead><tr>{headers}</tr></thead>
            <tbody>{rows}</tbody>
          </table>
        </div>
        """

    filter_html = f"""
    <form class="inline" method="get" action="/dataset_html">
      <label class="small">Datum van</label>
      <input type="date" name="date_from" value="{date_from or ''}">
      <label class="small">tot</label>
      <input type="date" name="date_to" value="{date_to or ''}">
      <label class="small">RFX Activity</label>
      <input type="text" name="rfx_activity" value="{rfx_activity or ''}" placeholder="bv. 4">
      <label class="small">Route (cnr_tour)</label>
      <input type="text" name="cnr_tour" value="{cnr_tour or ''}" placeholder="bv. 776907">
      <button type="submit" class="btn">Filter</button>
      <a href="/dataset_html" class="btn">Reset</a>
    </form>
    """

    body = f"""
    <h1>Dataset viewer</h1>
    <p class="sub">Ruwe data uit de tabel <code>orders</code>. Gebruik filters om subset te bekijken.</p>
    {filter_html}
    <br/>
    {table_html}
    """
    return _layout("Dataset", body)

# ------------------------------------------------------------
# ROUTES OVERVIEW + JIT
# ------------------------------------------------------------
@app.get("/routes_html", response_class=HTMLResponse)
def routes_html(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    rfx_activity: Optional[str] = Query(None),
    route_select: Optional[str] = Query(None),
    route_input: Optional[str] = Query(None),
):
    cnr_tour_filter = route_input.strip() if route_input else None
    if not cnr_tour_filter and route_select and route_select != "ALL":
        cnr_tour_filter = route_select

    df = load_orders(date_from, date_to, rfx_activity, cnr_tour_filter)

    if df.empty:
        routes_table = "<p class='sub'>Geen gegevens beschikbaar (controleer filters of upload eerst een dataset).</p>"
        route_options = '<option value="ALL">(geen selectie)</option>'
    else:
        unique_routes = df["cnr_tour"].dropna().astype(str).drop_duplicates().sort_values()
        route_options = '<option value="ALL">(geen selectie)</option>' + "".join(
            f'<option value="{r}" {"selected" if cnr_tour_filter==str(r) else ""}>{r}</option>'
            for r in unique_routes
        )

        route_rows = []
        for (date_dos, cnr_tour), g in df.groupby(["date_dos", "cnr_tour"]):
            work, deliveries = compute_jit(g)

            n_del = len(deliveries)
            s1_del = int(deliveries["jit_s1_delivery"].sum())
            s2_del = int(deliveries["jit_s2_delivery"].sum())
            s1_pct = (s1_del / n_del * 100.0) if n_del else 0.0
            s2_pct = (s2_del / n_del * 100.0) if n_del else 0.0

            n_ord = len(work)
            s1_ord = int(work["jit_s1_order"].sum())
            s2_ord = int(work["jit_s2_order"].sum())
            s1_ord_pct = (s1_ord / n_ord * 100.0) if n_ord else 0.0
            s2_ord_pct = (s2_ord / n_ord * 100.0) if n_ord else 0.0

            route_rows.append(
                dict(
                    date_dos=str(date_dos),
                    cnr_tour=str(cnr_tour),
                    deliveries=n_del,
                    jit_s1_del=s1_del,
                    jit_s2_del=s2_del,
                    jit_s1_del_pct=s1_pct,
                    jit_s2_del_pct=s2_pct,
                    orders=n_ord,
                    jit_s1_ord=s1_ord,
                    jit_s2_ord=s2_ord,
                    jit_s1_ord_pct=s1_ord_pct,
                    jit_s2_ord_pct=s2_ord_pct,
                )
            )

        if route_rows:
            r_df = pd.DataFrame(route_rows).sort_values(["date_dos", "cnr_tour"])

            headers = """
              <th>Datum</th>
              <th>Route</th>
              <th>Leveringen</th>
              <th>JIT S1 leveringen</th>
              <th>JIT S1 %</th>
              <th>JIT S2 leveringen</th>
              <th>JIT S2 %</th>
              <th>Orders</th>
              <th>JIT S1 orders</th>
              <th>JIT S1 %</th>
              <th>JIT S2 orders</th>
              <th>JIT S2 %</th>
              <th>Detail</th>
            """

            rows_html = ""
            for _, r in r_df.iterrows():
                if r["jit_s2_del_pct"] < 93:
                    cls = "route-bad"
                elif r["jit_s2_del_pct"] < 95:
                    cls = "route-mid"
                else:
                    cls = "route-good"

                link = f"/route_detail_html?date={r['date_dos']}&cnr_tour={r['cnr_tour']}&view=delivery"
                rows_html += f"""
                <tr class="{cls}">
                  <td class="mono">{r['date_dos']}</td>
                  <td class="mono">{r['cnr_tour']}</td>
                  <td class="mono">{int(r['deliveries'])}</td>
                  <td class="mono">{int(r['jit_s1_del'])}</td>
                  <td class="mono">{r['jit_s1_del_pct']:.2f}%</td>
                  <td class="mono">{int(r['jit_s2_del'])}</td>
                  <td class="mono">{r['jit_s2_del_pct']:.2f}%</td>
                  <td class="mono">{int(r['orders'])}</td>
                  <td class="mono">{int(r['jit_s1_ord'])}</td>
                  <td class="mono">{r['jit_s1_ord_pct']:.2f}%</td>
                  <td class="mono">{int(r['jit_s2_ord'])}</td>
                  <td class="mono">{r['jit_s2_ord_pct']:.2f}%</td>
                  <td><a class="btn" href="{link}">🔍 Detail</a></td>
                </tr>
                """

            routes_table = f"""
            <div class="topbar">
              <div class="sub">Aantal routes in overzicht: {len(r_df)}</div>
              <button class="copy-btn" onclick="copyTable('tblRoutes')">📋 Kopieer tabel</button>
            </div>
            <div class="table-wrapper">
              <table id="tblRoutes">
                <thead><tr>{headers}</tr></thead>
                <tbody>{rows_html}</tbody>
              </table>
            </div>
            """
        else:
            routes_table = "<p class='sub'>Geen routes gevonden binnen de filters.</p>"
            route_options = '<option value="ALL">(geen selectie)</option>'

    filter_html = f"""
    <form class="inline" method="get" action="/routes_html">
      <label class="small">Datum van</label>
      <input type="date" name="date_from" value="{date_from or ''}">
      <label class="small">tot</label>
      <input type="date" name="date_to" value="{date_to or ''}">
      <label class="small">RFX Activity</label>
      <input type="text" name="rfx_activity" value="{rfx_activity or ''}" placeholder="bv. 4 of 5">
      <label class="small">Route (select)</label>
      <select name="route_select">{route_options}</select>
      <label class="small">of vrije input</label>
      <input type="text" name="route_input" value="{route_input or ''}" placeholder="bv. 776907">
      <button type="submit" class="btn">Filter</button>
      <a href="/routes_html" class="btn">Reset</a>
    </form>
    """

    body = f"""
    <h1>Routes &amp; JIT analyse</h1>
    <p class="sub">
      Scenario 1: Actual tussen Win FROM en Win UNTIL.<br/>
      Scenario 2: Actual ≤ Win UNTIL (te vroeg is ook JIT).<br/>
      Gebruik filters om specifieke klanten (RFX Activity) of routes te analyseren.
    </p>
    {filter_html}
    <br/>
    {routes_table}
    """
    return _layout("Routes analyse", body)

# ------------------------------------------------------------
# ROUTE DETAIL (per leverpunt + per order)
# ------------------------------------------------------------
@app.get("/route_detail_html", response_class=HTMLResponse)
def route_detail_html(
    date: str,
    cnr_tour: str,
    view: str = Query("delivery", description="delivery|order"),
):
    df = load_orders(date_from=date, date_to=date, cnr_tour=cnr_tour)
    if df.empty:
        return _layout(
            "Route detail",
            f"<h2>Route {cnr_tour} – {date}</h2><p class='sub'>Geen data gevonden voor deze route.</p>",
        )

    work, deliveries = compute_jit(df)

    n_orders = len(work)
    s1_ord_pct = work["jit_s1_order"].mean() * 100 if n_orders else 0.0
    s2_ord_pct = work["jit_s2_order"].mean() * 100 if n_orders else 0.0

    work = work.sort_values(["planned_dt", "nm_short_unload"])
    deliveries = deliveries.sort_values(["first_planned_dt", "nm_short_unload"])

    if view == "order":
        rows_html = ""
        for _, r in work.iterrows():
            if r["jit_s2_order"]:
                row_class = "jit-ok" if r["jit_s1_order"] else "jit-late"
            else:
                row_class = "jit-root"

            rows_html += f"""
            <tr class="{row_class}">
              <td>{r.get("nm_short_unload","")}</td>
              <td>{r.get("RFX Activity","")}</td>
              <td>{r.get("RFX Year","")}</td>
              <td>{r.get("RFX Preperation","")}</td>
              <td class="mono">{_fmt_hhmm(r.get("Win FROM",""))}</td>
              <td class="mono">{_fmt_hhmm(r.get("Win UNTIL",""))}</td>
              <td class="mono">{_fmt_hhmm(r.get("Planned",""))}</td>
              <td class="mono">{_fmt_hhmm(r.get("Actual",""))}</td>
              <td>{"✔" if r["jit_s1_order"] else "✖"}</td>
              <td>{"✔" if r["jit_s2_order"] else "✖"}</td>
            </tr>
            """

        table_html = f"""
        <div class="topbar">
          <h3>Orderdetails (per order op de route)</h3>
          <button class="copy-btn" onclick="copyTable('tblRouteDetail')">📋 Kopieer tabel</button>
        </div>
        <div class="table-wrapper">
          <table id="tblRouteDetail">
            <thead>
              <tr>
                <th>Leverpunt</th>
                <th>RFX Activity</th>
                <th>RFX Year</th>
                <th>RFX Preperation</th>
                <th>Win FROM</th>
                <th>Win UNTIL</th>
                <th>Planned</th>
                <th>Actual</th>
                <th>JIT S1</th>
                <th>JIT S2</th>
              </tr>
            </thead>
            <tbody>{rows_html}</tbody>
          </table>
        </div>
        """
    else:
        rows_html = ""
        for _, r in deliveries.iterrows():
            if r["jit_s2_delivery"]:
                row_class = "jit-ok" if r["jit_s1_delivery"] else "jit-late"
            else:
                row_class = "jit-root"

            rows_html += f"""
            <tr class="{row_class}">
              <td>{r['nm_short_unload']}</td>
              <td class="mono">{int(r['orders'])}</td>
              <td class="mono">{_fmt_hhmm(r['win_from'])}</td>
              <td class="mono">{_fmt_hhmm(r['win_until'])}</td>
              <td class="mono">{_fmt_hhmm(r['first_planned'])}</td>
              <td class="mono">{_fmt_hhmm(r['first_actual'])}</td>
              <td>{"✔" if r["jit_s1_delivery"] else "✖"}</td>
              <td>{"✔" if r["jit_s2_delivery"] else "✖"}</td>
            </tr>
            """

        table_html = f"""
        <div class="topbar">
          <h3>Leveringen per winkelpunt / venster (geaggregeerd)</h3>
          <button class="copy-btn" onclick="copyTable('tblRouteDetail')">📋 Kopieer tabel</button>
        </div>
        <div class="table-wrapper">
          <table id="tblRouteDetail">
            <thead>
              <tr>
                <th>Leverpunt (nm_short_unload)</th>
                <th>Aantal orders</th>
                <th>Win FROM</th>
                <th>Win UNTIL</th>
                <th>Eerste Planned</th>
                <th>Eerste Actual</th>
                <th>JIT S1</th>
                <th>JIT S2</th>
              </tr>
            </thead>
            <tbody>{rows_html}</tbody>
          </table>
        </div>
        """

    body = f"""
    <h1>Route detail – {cnr_tour} op {date}</h1>
    <p class="sub">
      Scenario 1: <strong>Actual ≥ Win FROM</strong> en <strong>Actual ≤ Win UNTIL</strong>.<br/>
      Scenario 2: <strong>Actual ≤ Win UNTIL</strong> (te vroeg = ook JIT).<br/>
      Eén order binnen venster ⇒ volledige levering JIT.
    </p>
    <p class="sub">
      JIT S1 (orders): {s1_ord_pct:.2f}% &nbsp;&nbsp; JIT S2 (orders): {s2_ord_pct:.2f}%<br/>
      Groen = binnen S1, oranje = enkel binnen S2, rood = buiten S2.
    </p>
    <p>
      <a class="btn" href="/route_detail_html?date={date}&cnr_tour={cnr_tour}&view=delivery">🧱 Detail per leverpunt</a>
      &nbsp;
      <a class="btn" href="/route_detail_html?date={date}&cnr_tour={cnr_tour}&view=order">📦 Detail per order</a>
      &nbsp;
      <a class="btn" href="/routes_html">⬅️ Terug naar routes</a>
      &nbsp;
      <a class="btn" href="/transport_route_detail_html?date={date}&cnr_tour={cnr_tour}">🧭 Transport detail</a>
    </p>
    {table_html}
    """
    return _layout("Route detail", body)

# ------------------------------------------------------------
# WACHTTIJDEN – TOTAAL PER KLANT
# ------------------------------------------------------------
@app.get("/waits_html", response_class=HTMLResponse)
def waits_html(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    df = load_orders(date_from=date_from, date_to=date_to)

    if df.empty:
        body = """
        <h1>Analyse wachttijden (totaal per klant)</h1>
        <p class="sub">Geen gegevens beschikbaar (controleer filters of upload eerst een dataset).</p>
        <p><a href="/" class="btn">⬅️ Terug naar dashboard</a></p>
        """
        return _layout("Wachttijden per klant", body)

    df["DurationA_min"] = pd.to_numeric(df.get("DurationA", pd.NA), errors="coerce")
    df = df.dropna(subset=["DurationA_min"]).copy()
    if df.empty:
        body = """
        <h1>Analyse wachttijden (totaal per klant)</h1>
        <p class="sub">Er zijn geen rijen met DurationA (wachttijd) aanwezig.</p>
        <p><a href="/" class="btn">⬅️ Terug naar dashboard</a></p>
        """
        return _layout("Wachttijden per klant", body)

    # per levering
    delivery_grp = ["date_dos", "cnr_tour", "RFX Activity", "nm_short_unload"]
    deliveries = df.groupby(delivery_grp, as_index=False).agg(avg_wait_min=("DurationA_min", "mean"))

    # per klant
    cust_agg = (
        deliveries.groupby("RFX Activity", as_index=False)
        .agg(
            deliveries=("avg_wait_min", "size"),
            leverpunten=("nm_short_unload", "nunique"),
            total_wait_min=("avg_wait_min", "sum"),
            avg_wait_per_delivery=("avg_wait_min", "mean"),
        )
        .sort_values("total_wait_min", ascending=False)
    )

    headers = """
      <th>RFX Activity</th>
      <th>Aantal leveringen</th>
      <th>Aantal leverpunten</th>
      <th>Totaal wachttijd (min)</th>
      <th>Gem. wachttijd per levering (min)</th>
      <th>Detail</th>
    """
    rows_html = ""
    for _, r in cust_agg.iterrows():
        link = f"/waits_customer_detail_html?rfx_activity={r['RFX Activity']}&date_from={date_from or ''}&date_to={date_to or ''}"
        rows_html += f"""
        <tr>
          <td class="mono">{r['RFX Activity']}</td>
          <td class="mono">{int(r['deliveries'])}</td>
          <td class="mono">{int(r['leverpunten'])}</td>
          <td class="mono">{r['total_wait_min']:.1f}</td>
          <td class="mono">{r['avg_wait_per_delivery']:.1f}</td>
          <td><a class="btn" href="{link}">🔍 Detail</a></td>
        </tr>
        """

    table_html = f"""
    <div class="topbar">
      <div class="sub">Wachttijden per klant (RFX Activity), gesorteerd op totale wachttijd (hoog → laag).</div>
      <button class="copy-btn" onclick="copyTable('tblWaitsCust')">📋 Kopieer tabel</button>
    </div>
    <div class="table-wrapper">
      <table id="tblWaitsCust">
        <thead><tr>{headers}</tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
    """

    filter_html = f"""
    <form class="inline" method="get" action="/waits_html">
      <label class="small">Datum van</label>
      <input type="date" name="date_from" value="{date_from or ''}">
      <label class="small">tot</label>
      <input type="date" name="date_to" value="{date_to or ''}">
      <button type="submit" class="btn">Filter</button>
      <a href="/waits_html" class="btn">Reset</a>
    </form>
    """

    body = f"""
    <h1>Analyse wachttijden (totaal per klant)</h1>
    <p class="sub">
      Wachttijd = <strong>DurationA</strong> (minuten).<br/>
      1) Per levering (datum, route, klant, leverpunt) wordt het gemiddelde genomen.<br/>
      2) Per klant is totale wachttijd = som van die gemiddelden.
    </p>
    {filter_html}
    <br/>
    {table_html}
    <p style="margin-top:12px;"><a href="/" class="btn">⬅️ Terug naar dashboard</a></p>
    """
    return _layout("Wachttijden per klant", body)

@app.get("/waits_customer_detail_html", response_class=HTMLResponse)
def waits_customer_detail_html(
    rfx_activity: str,
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    df = load_orders(date_from=date_from, date_to=date_to, rfx_activity=rfx_activity)
    if df.empty:
        return _layout("Wachttijden detail", f"<h1>Wachttijden – detail klant {rfx_activity}</h1><p class='sub'>Geen data.</p>")

    df["DurationA_min"] = pd.to_numeric(df.get("DurationA", pd.NA), errors="coerce")
    df = df.dropna(subset=["DurationA_min"]).copy()
    if df.empty:
        return _layout("Wachttijden detail", f"<h1>Wachttijden – detail klant {rfx_activity}</h1><p class='sub'>Geen DurationA data.</p>")

    deliveries = (
        df.groupby(["date_dos", "cnr_tour", "nm_short_unload"], as_index=False)
        .agg(avg_wait_min=("DurationA_min", "mean"))
        .sort_values("avg_wait_min", ascending=False)
    )

    store_agg = (
        deliveries.groupby("nm_short_unload", as_index=False)
        .agg(
            deliveries=("avg_wait_min", "size"),
            total_wait_min=("avg_wait_min", "sum"),
        )
        .sort_values("total_wait_min", ascending=False)
    )

    headers1 = """
      <th>Leverpunt (nm_short_unload)</th>
      <th>Aantal leveringen</th>
      <th>Totaal wachttijd (min)</th>
      <th>Detail per order</th>
    """
    rows1 = ""
    for _, r in store_agg.iterrows():
        link = f"/waits_store_orders_html?rfx_activity={rfx_activity}&store={r['nm_short_unload']}&date_from={date_from or ''}&date_to={date_to or ''}"
        rows1 += f"""
        <tr>
          <td>{r['nm_short_unload']}</td>
          <td class="mono">{int(r['deliveries'])}</td>
          <td class="mono">{r['total_wait_min']:.1f}</td>
          <td><a class="btn" href="{link}">🔍 Orders</a></td>
        </tr>
        """

    table1 = f"""
    <div class="topbar">
      <h3>Wachttijden per leverpunt</h3>
      <button class="copy-btn" onclick="copyTable('tblWaitsStore')">📋 Kopieer tabel</button>
    </div>
    <div class="table-wrapper">
      <table id="tblWaitsStore">
        <thead><tr>{headers1}</tr></thead>
        <tbody>{rows1}</tbody>
      </table>
    </div>
    """

    headers2 = """
      <th>Datum</th>
      <th>Route</th>
      <th>Leverpunt</th>
      <th>Gem. wachttijd (min)</th>
      <th>Route detail</th>
      <th>Transport</th>
    """
    rows2 = ""
    for _, r in deliveries.iterrows():
        link = f"/route_detail_html?date={r['date_dos']}&cnr_tour={r['cnr_tour']}&view=delivery"
        tlink = f"/transport_route_detail_html?date={r['date_dos']}&cnr_tour={r['cnr_tour']}"
        rows2 += f"""
        <tr>
          <td class="mono">{r['date_dos']}</td>
          <td class="mono">{r['cnr_tour']}</td>
          <td>{r['nm_short_unload']}</td>
          <td class="mono">{r['avg_wait_min']:.1f}</td>
          <td><a class="btn" href="{link}">🔍 Route</a></td>
          <td><a class="btn" href="{tlink}">🧭 Transport</a></td>
        </tr>
        """

    table2 = f"""
    <div class="topbar">
      <h3>Wachttijden per levering (datum / route / leverpunt)</h3>
      <button class="copy-btn" onclick="copyTable('tblWaitsDeliveries')">📋 Kopieer tabel</button>
    </div>
    <div class="table-wrapper">
      <table id="tblWaitsDeliveries">
        <thead><tr>{headers2}</tr></thead>
        <tbody>{rows2}</tbody>
      </table>
    </div>
    """

    body = f"""
    <h1>Wachttijden – detail klant {rfx_activity}</h1>
    <p class="sub">Klik op 'Orders' om naar orderniveau te gaan voor een specifiek leverpunt.</p>
    {table1}
    <br/>
    {table2}
    <p style="margin-top:12px;">
      <a href="/waits_html" class="btn">⬅️ Terug naar wachttijden per klant</a>
      &nbsp;
      <a href="/transport_manager_html" class="btn">🧭 Naar transport overzicht</a>
    </p>
    """
    return _layout("Wachttijden detail", body)

@app.get("/waits_store_orders_html", response_class=HTMLResponse)
def waits_store_orders_html(
    rfx_activity: str,
    store: str,
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    df = load_orders(date_from=date_from, date_to=date_to, rfx_activity=rfx_activity)
    if df.empty:
        return _layout("Wachttijden orders", f"<h1>Geen data</h1><p class='sub'>Geen gegevens.</p>")

    df = df[df["nm_short_unload"] == store].copy()
    df["DurationA_min"] = pd.to_numeric(df.get("DurationA", pd.NA), errors="coerce")
    df = df.dropna(subset=["DurationA_min"]).copy()
    if df.empty:
        return _layout("Wachttijden orders", f"<h1>Geen DurationA</h1><p class='sub'>Geen wachttijden.</p>")

    df["planned_dt"] = _combine_datetime(df, "date_dos", "Planned")
    df = df.sort_values(["date_dos", "cnr_tour", "planned_dt", "Actual"])

    headers = """
      <th>Datum</th>
      <th>Route</th>
      <th>Order</th>
      <th>Leverpunt</th>
      <th>Win FROM</th>
      <th>Win UNTIL</th>
      <th>Planned</th>
      <th>Actual</th>
      <th>A_Depart</th>
      <th>Wachttijd (min)</th>
      <th>Route detail (orders)</th>
      <th>Transport</th>
    """
    rows = ""
    for _, r in df.iterrows():
        link = f"/route_detail_html?date={r['date_dos']}&cnr_tour={r['cnr_tour']}&view=order"
        tlink = f"/transport_route_detail_html?date={r['date_dos']}&cnr_tour={r['cnr_tour']}"
        rows += f"""
        <tr>
          <td class="mono">{r['date_dos']}</td>
          <td class="mono">{r['cnr_tour']}</td>
          <td class="mono">{r['cnr_cust']}</td>
          <td>{r['nm_short_unload']}</td>
          <td class="mono">{_fmt_hhmm(r['Win FROM'])}</td>
          <td class="mono">{_fmt_hhmm(r['Win UNTIL'])}</td>
          <td class="mono">{_fmt_hhmm(r['Planned'])}</td>
          <td class="mono">{_fmt_hhmm(r['Actual'])}</td>
          <td class="mono">{_fmt_hhmm(r.get('A_Depart',''))}</td>
          <td class="mono">{r['DurationA_min']:.1f}</td>
          <td><a class="btn" href="{link}">🔍 Route</a></td>
          <td><a class="btn" href="{tlink}">🧭 Transport</a></td>
        </tr>
        """

    table_html = f"""
    <div class="topbar">
      <h3>Orders met wachttijd – klant {rfx_activity}, leverpunt {store}</h3>
      <button class="copy-btn" onclick="copyTable('tblWaitsStoreOrders')">📋 Kopieer tabel</button>
    </div>
    <div class="table-wrapper">
      <table id="tblWaitsStoreOrders">
        <thead><tr>{headers}</tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
    """

    body = f"""
    <h1>Wachttijden – orders voor klant {rfx_activity}, leverpunt {store}</h1>
    {table_html}
    <p style="margin-top:12px;">
      <a href="/waits_customer_detail_html?rfx_activity={rfx_activity}&date_from={date_from or ''}&date_to={date_to or ''}" class="btn">⬅️ Terug</a>
    </p>
    """
    return _layout("Wachttijden orders", body)

# ------------------------------------------------------------
# Outside JIT helpers (S2): stop-level table
# ------------------------------------------------------------
def _stop_level_outside_s2(df: pd.DataFrame) -> pd.DataFrame:
    """
    Bouw 1 rij per levering = (date_dos, cnr_tour, nm_short_unload).
    late_minutes = max(0, Actual - Win UNTIL) in minuten.
    outside = True als Actual > Win UNTIL, of als Actual/Win UNTIL ontbreekt.
    """
    if df.empty:
        return df

    tmp = df.copy()
    tmp["actual_dt"] = _combine_datetime(tmp, "date_dos", "Actual") if "Actual" in tmp.columns else pd.NaT
    tmp["win_until_dt"] = _combine_datetime(tmp, "date_dos", "Win UNTIL") if "Win UNTIL" in tmp.columns else pd.NaT
    tmp["planned_dt"] = _combine_datetime(tmp, "date_dos", "Planned") if "Planned" in tmp.columns else pd.NaT

    grp = ["date_dos", "cnr_tour", "nm_short_unload"]
    stops = (
        tmp.groupby(grp, as_index=False)
        .agg(
            rfx_activity=("RFX Activity", "first"),
            actual_dt=("actual_dt", "min"),
            win_until_dt=("win_until_dt", "first"),
            planned_dt=("planned_dt", "min"),
            actual=("Actual", "first"),
            win_until=("Win UNTIL", "first"),
            planned=("Planned", "first"),
            orders=("cnr_cust", "count"),
        )
    )

    diff = (stops["actual_dt"] - stops["win_until_dt"])
    late_min = diff.dt.total_seconds() / 60.0
    stops["late_minutes"] = late_min

    stops["outside_s2"] = False
    has_both = stops["actual_dt"].notna() & stops["win_until_dt"].notna()
    stops.loc[has_both, "outside_s2"] = stops.loc[has_both, "actual_dt"] > stops.loc[has_both, "win_until_dt"]
    stops.loc[~has_both, "outside_s2"] = True

    stops.loc[has_both, "late_minutes"] = stops.loc[has_both, "late_minutes"].apply(lambda v: max(0.0, float(v)) if pd.notna(v) else pd.NA)

    stops["actual"] = stops["actual"].apply(_fmt_hhmm)
    stops["win_until"] = stops["win_until"].apply(_fmt_hhmm)
    stops["planned"] = stops["planned"].apply(_fmt_hhmm)

    return stops

# ------------------------------------------------------------
# JIT OUTSIDE (S2) – per dag aantal leverpunten buiten JIT + detail link
# ------------------------------------------------------------
@app.get("/jit_outside_daily_html", response_class=HTMLResponse)
def jit_outside_daily_html(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    rfx_activity: Optional[str] = Query(None),
):
    df = load_orders(date_from=date_from, date_to=date_to, rfx_activity=rfx_activity)
    stops = _stop_level_outside_s2(df)

    filter_html = f"""
    <form class="inline" method="get" action="/jit_outside_daily_html">
      <label class="small">Datum van</label>
      <input type="date" name="date_from" value="{date_from or ''}">
      <label class="small">tot</label>
      <input type="date" name="date_to" value="{date_to or ''}">
      <label class="small">RFX Activity</label>
      <input type="text" name="rfx_activity" value="{rfx_activity or ''}" placeholder="bv. 4 of 5">
      <button type="submit" class="btn">Filter</button>
      <a href="/jit_outside_daily_html" class="btn">Reset</a>
    </form>
    """

    if stops.empty:
        return _layout("JIT outside – daily", f"<h1>Leverpunten buiten JIT (S2) – per dag</h1><p class='sub'>Geen data.</p>{filter_html}")

    daily = (
        stops.groupby("date_dos", as_index=False)
        .agg(
            leverpunten=("nm_short_unload", "nunique"),
            leverpunten_outside=("outside_s2", lambda s: int(s.sum())),
        )
        .sort_values("date_dos", ascending=True)
    )
    daily["outside_pct"] = (daily["leverpunten_outside"] / daily["leverpunten"].replace({0: pd.NA}) * 100.0)

    headers = """
      <th>Datum</th>
      <th># Leverpunten</th>
      <th># Buiten JIT (S2)</th>
      <th>% Buiten JIT</th>
      <th>Detail</th>
    """
    rows_html = ""
    for _, r in daily.iterrows():
        link = f"/jit_outside_points_html?date={r['date_dos']}&rfx_activity={rfx_activity or ''}"
        rows_html += f"""
        <tr>
          <td class="mono">{r['date_dos']}</td>
          <td class="mono">{int(r['leverpunten'])}</td>
          <td class="mono">{int(r['leverpunten_outside'])}</td>
          <td class="mono">{(0.0 if pd.isna(r['outside_pct']) else r['outside_pct']):.2f}%</td>
          <td><a class="btn" href="{link}">🔍 Leverpunten</a></td>
        </tr>
        """

    table_html = f"""
    <div class="topbar">
      <div>
        <h3 style="margin:0">Overzicht per dag</h3>
        <div class="sub">Buiten JIT (S2) = Actual &gt; Win UNTIL (of missing times)</div>
      </div>
      <button class="copy-btn" onclick="copyTable('tblOutsideDaily')">📋 Kopieer tabel</button>
    </div>
    <div class="table-wrapper">
      <table id="tblOutsideDaily">
        <thead><tr>{headers}</tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
    """

    body = f"""
    <h1>Leverpunten buiten JIT (S2) – per dag</h1>
    <p class="sub">Scenario 2 JIT = <strong>Actual ≤ Win UNTIL</strong>. Buiten JIT = <strong>Actual &gt; Win UNTIL</strong>.</p>
    {filter_html}
    <br/>
    {table_html}
    <p style="margin-top:14px"><a class="btn" href="/">⬅️ Dashboard</a></p>
    """
    return _layout("JIT outside – daily", body)

# ------------------------------------------------------------
# Detail: alle leverpunten buiten JIT op een dag (optioneel per klant) + link naar route
# ------------------------------------------------------------
@app.get("/jit_outside_points_html", response_class=HTMLResponse)
def jit_outside_points_html(
    date: str,
    rfx_activity: Optional[str] = Query(None),
):
    df = load_orders(date_from=date, date_to=date, rfx_activity=(rfx_activity or None))
    stops = _stop_level_outside_s2(df)
    if stops.empty:
        return _layout("Outside leverpunten", f"<h1>Outside JIT – {date}</h1><p class='sub'>Geen data.</p><p><a class='btn' href='/jit_outside_daily_html'>⬅️ Terug</a></p>")

    outside = stops[stops["outside_s2"] == True].copy()  # noqa: E712
    outside = outside.sort_values(["late_minutes"], ascending=False)

    headers = """
      <th>Route</th>
      <th>Leverpunt</th>
      <th>RFX Activity</th>
      <th>Planned</th>
      <th>Actual</th>
      <th>Win UNTIL</th>
      <th>Min te laat</th>
      <th>Orders</th>
      <th>Route detail</th>
      <th>Transport</th>
    """
    rows_html = ""
    for _, r in outside.iterrows():
        link = f"/route_detail_html?date={date}&cnr_tour={r['cnr_tour']}&view=delivery"
        tlink = f"/transport_route_detail_html?date={date}&cnr_tour={r['cnr_tour']}"
        late = "" if pd.isna(r["late_minutes"]) else f"{float(r['late_minutes']):.0f}"
        rows_html += f"""
        <tr class="jit-root">
          <td class="mono">{r['cnr_tour']}</td>
          <td>{r['nm_short_unload']}</td>
          <td class="mono">{r.get('rfx_activity','')}</td>
          <td class="mono">{r.get('planned','')}</td>
          <td class="mono">{r.get('actual','')}</td>
          <td class="mono">{r.get('win_until','')}</td>
          <td class="mono">{late}</td>
          <td class="mono">{int(r.get('orders',0))}</td>
          <td><a class="btn" href="{link}">🔍 Route</a></td>
          <td><a class="btn" href="{tlink}">🧭 Transport</a></td>
        </tr>
        """

    table_html = f"""
    <div class="topbar">
      <div>
        <h3 style="margin:0">Leverpunten buiten JIT (S2) – {date}</h3>
        <div class="sub">Gesorteerd op grootste lateness (minuten).</div>
      </div>
      <button class="copy-btn" onclick="copyTable('tblOutsidePoints')">📋 Kopieer tabel</button>
    </div>
    <div class="table-wrapper">
      <table id="tblOutsidePoints">
        <thead><tr>{headers}</tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
    """

    back = "/jit_outside_daily_html"
    body = f"""
    <h1>Outside JIT – leverpunten</h1>
    <p class="sub">Datum: <strong>{date}</strong>{f" – RFX Activity {rfx_activity}" if rfx_activity else ""}</p>
    {table_html}
    <p style="margin-top:14px">
      <a class="btn" href="{back}">⬅️ Terug naar daily</a>
      &nbsp;
      <a class="btn" href="/">🏠 Dashboard</a>
    </p>
    """
    return _layout("Outside leverpunten", body)

# ------------------------------------------------------------
# Buckets outside JIT per dag + % + cumulatief tov "Huidige JIT%"
# ------------------------------------------------------------
@app.get("/outside_jit_daily_html", response_class=HTMLResponse)
def outside_jit_daily_html(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    rfx_activity: Optional[str] = Query(None),
):
    df = load_orders(date_from=date_from, date_to=date_to, rfx_activity=rfx_activity)
    stops = _stop_level_outside_s2(df)

    filter_html = f"""
    <form class="inline" method="get" action="/outside_jit_daily_html">
      <label class="small">Datum van</label>
      <input type="date" name="date_from" value="{date_from or ''}">
      <label class="small">tot</label>
      <input type="date" name="date_to" value="{date_to or ''}">
      <label class="small">RFX Activity</label>
      <input type="text" name="rfx_activity" value="{rfx_activity or ''}" placeholder="bv. 4 of 5">
      <button type="submit" class="btn">Filter</button>
      <a href="/outside_jit_daily_html" class="btn">Reset</a>
    </form>
    """

    if stops.empty:
        return _layout("Buckets outside JIT", f"<h1>Analyse buiten JIT per dag (buckets)</h1><p class='sub'>Geen data.</p>{filter_html}")

    deliveries_all = stops.groupby(["date_dos", "cnr_tour", "nm_short_unload"], as_index=False).size()
    totals_day = deliveries_all.groupby("date_dos", as_index=False).size().rename(columns={"size": "total_deliveries"})

    outside = stops[stops["outside_s2"] == True].copy()  # noqa: E712

    def bucket(v: object) -> str:
        if v is None or pd.isna(v):
            return "unknown"
        x = float(v)
        if x <= 15:
            return "0-15"
        if x <= 30:
            return "15-30"
        if x <= 45:
            return "30-45"
        if x <= 60:
            return "45-60"
        return "60+"

    outside["bucket"] = outside["late_minutes"].apply(bucket)
    labels = ["0-15", "15-30", "30-45", "45-60", "60+"]

    summary = (
        outside.groupby(["date_dos", "bucket"], as_index=False)
        .size()
        .rename(columns={"size": "outside_cnt"})
    )

    pivot_cnt = (
        summary.pivot(index="date_dos", columns="bucket", values="outside_cnt")
        .fillna(0)
        .astype(int)
        .reset_index()
    )
    for c in labels + ["unknown"]:
        if c not in pivot_cnt.columns:
            pivot_cnt[c] = 0

    pivot = pivot_cnt.merge(totals_day, on="date_dos", how="left")
    pivot["total_deliveries"] = pivot["total_deliveries"].fillna(0).astype(int)

    pivot["Totaal buiten JIT"] = pivot[labels + ["unknown"]].sum(axis=1)

    pivot["Huidige JIT%"] = pivot.apply(
        lambda r: ((r["total_deliveries"] - r["Totaal buiten JIT"]) / r["total_deliveries"] * 100.0)
        if r["total_deliveries"] else 0.0,
        axis=1,
    )

    for b in labels:
        pivot[f"{b} %"] = pivot.apply(
            lambda r: (r[b] / r["total_deliveries"] * 100.0) if r["total_deliveries"] else 0.0,
            axis=1,
        )

    def row_cum(row: pd.Series) -> pd.Series:
        cum = 0.0
        out = {}
        for b in labels:
            cum += float(row[f"{b} %"])
            out[f"JIT% cumul t/m {b}"] = float(row["Huidige JIT%"]) + cum
        return pd.Series(out)

    pivot = pd.concat([pivot, pivot.apply(row_cum, axis=1)], axis=1)

    ordered_cols = (
        ["date_dos", "total_deliveries", "Totaal buiten JIT", "Huidige JIT%"]
        + sum([[b, f"{b} %", f"JIT% cumul t/m {b}"] for b in labels], [])
        + ["unknown"]
    )
    pivot = pivot[ordered_cols].sort_values("date_dos", ascending=True)

    headers = "".join(f"<th>{c}</th>" for c in pivot.columns)
    rows = ""
    for _, r in pivot.iterrows():
        cells = ""
        for c in pivot.columns:
            v = r[c]
            if isinstance(v, float):
                if c.endswith("%") or c.startswith("JIT%"):
                    cells += f"<td class='mono'>{v:.2f}%</td>"
                else:
                    cells += f"<td class='mono'>{v:.2f}</td>"
            else:
                cells += f"<td class='mono'>{v}</td>"
        date_s = r["date_dos"]
        link = f"/jit_outside_points_html?date={date_s}&rfx_activity={rfx_activity or ''}"
        cells += f"<td><a class='btn' href='{link}'>🔍 Detail</a></td>"
        rows += f"<tr>{cells}</tr>"

    table_html = f"""
    <div class="topbar">
      <div>
        <h3 style="margin:0">Buckets buiten JIT per dag</h3>
        <div class="sub">Cumulatief = Huidige JIT% + bucket% (zoals gevraagd). 'unknown' = missing times.</div>
      </div>
      <button class="copy-btn" onclick="copyTable('tblBucketsDaily')">📋 Kopieer tabel</button>
    </div>
    <div class="table-wrapper">
      <table id="tblBucketsDaily">
        <thead><tr>{headers}<th>Detail</th></tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
    """

    body = f"""
    <h1>Analyse buiten JIT per dag (buckets)</h1>
    <p class="sub">
      Buiten JIT (S2) = Actual &gt; Win UNTIL (of ontbrekende tijden).<br/>
      Buckets: 0–15 / 15–30 / 30–45 / 45–60 / 60+ (min te laat).<br/>
      Cumulatieve kolommen tonen: <strong>Huidige JIT%</strong> + <strong>cumul bucket%</strong>.
    </p>
    {filter_html}
    <br/>
    {table_html}
    <p style="margin-top:14px"><a class="btn" href="/">⬅️ Dashboard</a></p>
    """
    return _layout("Buckets outside JIT", body)

# ============================================================
# RCA – Delay drivers (proxy) + detail
# ============================================================
def _minutes(td: pd.Timedelta) -> float:
    if pd.isna(td):
        return float("nan")
    return td.total_seconds() / 60.0

def _safe_dt(date_s: str, time_s: object) -> pd.Timestamp:
    if pd.isna(time_s):
        return pd.NaT
    s = str(time_s).strip()
    if s.lower() in ("", "nan", "none"):
        return pd.NaT
    if len(s) == 5 and s[2] == ":":
        s = s + ":00"
    return pd.to_datetime(f"{date_s} {s}", errors="coerce")

@app.get("/rca_delay_drivers_html", response_class=HTMLResponse)
def rca_delay_drivers_html(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    rfx_activity: Optional[str] = Query(None),
):
    df = load_orders(date_from=date_from, date_to=date_to, rfx_activity=rfx_activity)

    filter_html = f"""
    <form class="inline" method="get" action="/rca_delay_drivers_html">
      <label class="small">Datum van</label>
      <input type="date" name="date_from" value="{date_from or ''}">
      <label class="small">tot</label>
      <input type="date" name="date_to" value="{date_to or ''}">
      <label class="small">RFX Activity</label>
      <input type="text" name="rfx_activity" value="{rfx_activity or ''}" placeholder="bv. 4 of 5">
      <button type="submit" class="btn">Filter</button>
      <a href="/rca_delay_drivers_html" class="btn">Reset</a>
    </form>
    """

    if df.empty:
        return _layout("RCA – Delay drivers", f"<h1>RCA – Delay drivers</h1><p class='sub'>Geen data.</p>{filter_html}")

    # stop-level per route/leverpunt (1 rij per leverpunt)
    stop_rows = []
    has_durationA = "DurationA" in df.columns
    for (d, tour, shop, act), g in df.groupby(["date_dos", "cnr_tour", "nm_short_unload", "RFX Activity"], dropna=False):
        d = str(d)
        planned_dt = _safe_dt(d, g["Planned"].iloc[0] if "Planned" in g.columns else None)
        actual_dt = _safe_dt(d, g["Actual"].iloc[0] if "Actual" in g.columns else None)
        a_depart_dt = _safe_dt(d, g["A_Depart"].iloc[0] if "A_Depart" in g.columns else None)
        win_until_dt = _safe_dt(d, g["Win UNTIL"].iloc[0] if "Win UNTIL" in g.columns else None)

        if has_durationA:
            w = pd.to_numeric(g["DurationA"].iloc[0], errors="coerce")
            wait_min = float(w) if pd.notna(w) else float("nan")
        else:
            wait_min = _minutes(a_depart_dt - actual_dt)

        late_min = _minutes(actual_dt - win_until_dt)
        late_min = max(0.0, late_min) if pd.notna(late_min) else float("nan")

        stop_rows.append(
            dict(
                date_dos=d,
                cnr_tour=str(tour),
                rfx_activity=str(act),
                nm_short_unload=str(shop),
                planned_dt=planned_dt,
                actual_dt=actual_dt,
                a_depart_dt=a_depart_dt,
                planned=_fmt_hhmm(g["Planned"].iloc[0]) if "Planned" in g.columns else "",
                actual=_fmt_hhmm(g["Actual"].iloc[0]) if "Actual" in g.columns else "",
                a_depart=_fmt_hhmm(g["A_Depart"].iloc[0]) if "A_Depart" in g.columns else "",
                win_until=_fmt_hhmm(g["Win UNTIL"].iloc[0]) if "Win UNTIL" in g.columns else "",
                wait_min=wait_min,
                late_min=late_min,
                orders=len(g),
            )
        )

    stops = pd.DataFrame(stop_rows)
    if stops.empty:
        return _layout("RCA – Delay drivers", f"<h1>RCA – Delay drivers</h1><p class='sub'>Geen stopdata.</p>{filter_html}")

    stops = stops.sort_values(["date_dos", "cnr_tour", "planned_dt", "nm_short_unload"])

    # proxy decompositie
    tmp = stops.copy()
    tmp["prev_a_depart"] = tmp.groupby(["date_dos", "cnr_tour"])["a_depart_dt"].shift(1)
    tmp["prev_planned"] = tmp.groupby(["date_dos", "cnr_tour"])["planned_dt"].shift(1)

    tmp["transit_actual_min"] = (tmp["actual_dt"] - tmp["prev_a_depart"]).apply(_minutes)
    tmp["transit_planned_min"] = (tmp["planned_dt"] - tmp["prev_planned"]).apply(_minutes)
    tmp["transit_delay_min"] = tmp["transit_actual_min"] - tmp["transit_planned_min"]
    tmp["late_departure_proxy_min"] = (tmp["prev_a_depart"] - tmp["prev_planned"]).apply(_minutes)

    route_decomp = (
        tmp.dropna(subset=["prev_a_depart", "prev_planned"])
        .groupby(["date_dos", "cnr_tour", "rfx_activity"], as_index=False)
        .agg(
            stops=("nm_short_unload", "count"),
            total_wait_min=("wait_min", "sum"),
            total_late_min=("late_min", "sum"),
            sum_late_depart_proxy_min=("late_departure_proxy_min", "sum"),
            sum_transit_delay_min=("transit_delay_min", "sum"),
        )
        .sort_values(["sum_late_depart_proxy_min", "sum_transit_delay_min"], ascending=[False, False])
    )

    def _bucket(x: float) -> str:
        if pd.isna(x):
            return "unknown"
        if x == 0:
            return "0"
        if 0 < x <= 15:
            return "0–15"
        if 15 < x <= 30:
            return "15–30"
        if 30 < x <= 60:
            return "30–60"
        return "60+"

    stops["late_bucket"] = stops["late_min"].apply(_bucket)
    buckets = (
        stops.groupby(["date_dos", "rfx_activity", "late_bucket"], as_index=False)
        .agg(leverpunten=("nm_short_unload", "nunique"))
        .sort_values(["date_dos", "rfx_activity", "late_bucket"])
    )

    cust_day = (
        stops.groupby(["date_dos", "rfx_activity"], as_index=False)
        .agg(
            leverpunten=("nm_short_unload", "nunique"),
            routes=("cnr_tour", "nunique"),
            total_wait_min=("wait_min", "sum"),
            total_late_min=("late_min", "sum"),
        )
        .sort_values(["date_dos", "total_wait_min"], ascending=[True, False])
    )

    # klant/dag table met detail knop
    cust_headers = """
      <th>Datum</th>
      <th>RFX Activity</th>
      <th>Leverpunten</th>
      <th>Routes</th>
      <th>Totaal wachttijd (min)</th>
      <th>Totaal te laat (min)</th>
      <th>Detail</th>
    """
    cust_rows = ""
    for _, r in cust_day.iterrows():
        link = f"/rca_delay_drivers_detail_html?date={r['date_dos']}&rfx_activity={r['rfx_activity']}"
        cust_rows += f"""
        <tr>
          <td class="mono">{r['date_dos']}</td>
          <td class="mono">{r['rfx_activity']}</td>
          <td class="mono">{int(r['leverpunten'])}</td>
          <td class="mono">{int(r['routes'])}</td>
          <td class="mono">{float(r['total_wait_min']):.0f}</td>
          <td class="mono">{float(r['total_late_min']):.0f}</td>
          <td><a class="btn" href="{link}">🔍 Detail</a></td>
        </tr>
        """

    cust_table = f"""
    <div class="topbar">
      <div>
        <h3 style="margin:0">Wachttijden per dag per klant</h3>
        <div class="sub">Klik door naar detail per klant/dag (leverpunten + route links).</div>
      </div>
      <button class="copy-btn" onclick="copyTable('tblRcaCustDay')">📋 Kopieer tabel</button>
    </div>
    <div class="table-wrapper">
      <table id="tblRcaCustDay">
        <thead><tr>{cust_headers}</tr></thead>
        <tbody>{cust_rows}</tbody>
      </table>
    </div>
    """

    def _render_df(df_in: pd.DataFrame, table_id: str, title: str, hint: str) -> str:
        if df_in.empty:
            return f"<h3>{title}</h3><p class='sub'>Geen data.</p>"
        headers = "".join(f"<th>{c}</th>" for c in df_in.columns)
        rows = ""
        for _, rr in df_in.iterrows():
            cells = "".join(f"<td class='mono'>{'' if pd.isna(rr[c]) else rr[c]}</td>" for c in df_in.columns)
            rows += f"<tr>{cells}</tr>"
        return f"""
        <div class="topbar">
          <div>
            <h3 style="margin:0">{title}</h3>
            <div class="sub">{hint}</div>
          </div>
          <button class="copy-btn" onclick="copyTable('{table_id}')">📋 Kopieer tabel</button>
        </div>
        <div class="table-wrapper">
          <table id="{table_id}">
            <thead><tr>{headers}</tr></thead>
            <tbody>{rows}</tbody>
          </table>
        </div>
        """

    body = f"""
    <h1>RCA – Delay drivers</h1>
    <p class="sub">
      • Wachttijd = DurationA (indien aanwezig) anders A_Depart − Actual.<br/>
      • “Te laat” = max(0, Actual − Win UNTIL) in minuten (S2 referentie).<br/>
      • Late departure vs transit is een proxy-decompositie op basis van stop-sequence (planned order).
    </p>
    {filter_html}
    <br/>
    {cust_table}

    <h2>1) Late departure vs transit (per route)</h2>
    {_render_df(route_decomp.round(2), "tblRcaDecomp", "Routes – delay drivers (proxy)", "Gesorteerd op late-departure proxy, daarna transit delay.")}

    <h2>2) Minuten te laat buckets</h2>
    {_render_df(buckets, "tblRcaBuckets", "Buckets te laat (leverpunten)", "Buckets t.o.v. Win UNTIL: 0–15 / 15–30 / 30–60 / 60+.")}

    <p style="margin-top:14px"><a class="btn" href="/">⬅️ Dashboard</a></p>
    """
    return _layout("RCA – Delay drivers", body)

@app.get("/rca_delay_drivers_detail_html", response_class=HTMLResponse)
def rca_delay_drivers_detail_html(date: str, rfx_activity: str):
    df = load_orders(date_from=date, date_to=date, rfx_activity=rfx_activity)
    if df.empty:
        return _layout("RCA detail", f"<h1>RCA detail</h1><p class='sub'>Geen data voor {date} / RFX {rfx_activity}</p>")

    rows = []
    has_durationA = "DurationA" in df.columns
    for (tour, shop), g in df.groupby(["cnr_tour", "nm_short_unload"], dropna=False):
        d = str(g["date_dos"].iloc[0])
        planned_dt = _safe_dt(d, g["Planned"].iloc[0] if "Planned" in g.columns else None)
        actual_dt = _safe_dt(d, g["Actual"].iloc[0] if "Actual" in g.columns else None)
        a_depart_dt = _safe_dt(d, g["A_Depart"].iloc[0] if "A_Depart" in g.columns else None)
        win_until_dt = _safe_dt(d, g["Win UNTIL"].iloc[0] if "Win UNTIL" in g.columns else None)

        if has_durationA:
            w = pd.to_numeric(g["DurationA"].iloc[0], errors="coerce")
            wait_min = float(w) if pd.notna(w) else float("nan")
        else:
            wait_min = _minutes(a_depart_dt - actual_dt)

        late_min = _minutes(actual_dt - win_until_dt)
        late_min = max(0.0, late_min) if pd.notna(late_min) else float("nan")

        rows.append(
            dict(
                cnr_tour=str(tour),
                nm_short_unload=str(shop),
                orders=len(g),
                planned=_fmt_hhmm(g["Planned"].iloc[0]) if "Planned" in g.columns else "",
                actual=_fmt_hhmm(g["Actual"].iloc[0]) if "Actual" in g.columns else "",
                a_depart=_fmt_hhmm(g["A_Depart"].iloc[0]) if "A_Depart" in g.columns else "",
                win_until=_fmt_hhmm(g["Win UNTIL"].iloc[0]) if "Win UNTIL" in g.columns else "",
                wait_min=wait_min,
                late_min=late_min,
                planned_dt=planned_dt,
            )
        )

    stops = pd.DataFrame(rows).sort_values(["wait_min"], ascending=[False])

    headers = """
      <th>Route</th>
      <th>Leverpunt</th>
      <th>Orders</th>
      <th>Planned</th>
      <th>Actual</th>
      <th>A_Depart</th>
      <th>Win UNTIL</th>
      <th>Wachttijd (min)</th>
      <th>Te laat (min)</th>
      <th>Route detail</th>
      <th>Transport</th>
    """
    tr = ""
    for _, r in stops.iterrows():
        link = f"/route_detail_html?date={date}&cnr_tour={r['cnr_tour']}&view=delivery"
        tlink = f"/transport_route_detail_html?date={date}&cnr_tour={r['cnr_tour']}"
        tr += f"""
        <tr>
          <td class="mono">{r['cnr_tour']}</td>
          <td>{r['nm_short_unload']}</td>
          <td class="mono">{int(r['orders'])}</td>
          <td class="mono">{r['planned']}</td>
          <td class="mono">{r['actual']}</td>
          <td class="mono">{r['a_depart']}</td>
          <td class="mono">{r['win_until']}</td>
          <td class="mono">{"" if pd.isna(r['wait_min']) else f"{float(r['wait_min']):.0f}"}</td>
          <td class="mono">{"" if pd.isna(r['late_min']) else f"{float(r['late_min']):.0f}"}</td>
          <td><a class="btn" href="{link}">🔍 Route</a></td>
          <td><a class="btn" href="{tlink}">🧭 Transport</a></td>
        </tr>
        """

    table = f"""
    <div class="topbar">
      <div>
        <h3 style="margin:0">Leverpunten (wachttijd groot → klein)</h3>
        <div class="sub">Drill-down naar bestaande route detail & transport detail.</div>
      </div>
      <button class="copy-btn" onclick="copyTable('tblRcaDetail')">📋 Kopieer tabel</button>
    </div>
    <div class="table-wrapper">
      <table id="tblRcaDetail">
        <thead><tr>{headers}</tr></thead>
        <tbody>{tr}</tbody>
      </table>
    </div>
    """

    body = f"""
    <h1>RCA detail – {date} – RFX Activity {rfx_activity}</h1>
    <p class="sub">Overzicht per leverpunt (gegroepeerd), gesorteerd op hoogste wachttijd.</p>
    <p>
      <a class="btn" href="/rca_delay_drivers_html">⬅️ Terug naar RCA</a>
      &nbsp;
      <a class="btn" href="/">🏠 Dashboard</a>
    </p>
    {table}
    """
    return _layout("RCA detail", body)

# ============================================================
# TRANSPORT MANAGER ANALYSE
# ============================================================
def _minutes_between(a: pd.Timestamp, b: pd.Timestamp) -> float:
    if pd.isna(a) or pd.isna(b):
        return float("nan")
    return (a - b).total_seconds() / 60.0

def _route_departure_delay(df_route: pd.DataFrame) -> float:
    """
    Route-level: A_Depart vs P_Depart (minuten).
    Neemt eerste niet-lege waarde in de route.
    """
    if df_route.empty:
        return float("nan")

    d = str(df_route["date_dos"].iloc[0])

    p = None
    a = None
    if "P_Depart" in df_route.columns and df_route["P_Depart"].notna().any():
        p = df_route.loc[df_route["P_Depart"].notna(), "P_Depart"].iloc[0]
    if "A_Depart" in df_route.columns and df_route["A_Depart"].notna().any():
        a = df_route.loc[df_route["A_Depart"].notna(), "A_Depart"].iloc[0]

    if p is None or a is None:
        return float("nan")

    p_dt = pd.to_datetime(f"{d} {_fmt_hhmm(p)}", format="%Y-%m-%d %H:%M", errors="coerce")
    a_dt = pd.to_datetime(f"{d} {_fmt_hhmm(a)}", format="%Y-%m-%d %H:%M", errors="coerce")
    return _minutes_between(a_dt, p_dt)

def _stop_level_for_transport(df: pd.DataFrame) -> pd.DataFrame:
    """
    1 rij per leverpunt (date_dos, cnr_tour, nm_short_unload)
    - planned_dt, actual_dt, a_depart_dt
    - planned_block_min (DurationP)
    - actual_block_min (DurationA of fallback A_Depart - Actual)
    - seq planned vs actual
    """
    if df.empty:
        return df

    tmp = df.copy()
    tmp["planned_dt"] = _combine_datetime(tmp, "date_dos", "Planned") if "Planned" in tmp.columns else pd.NaT
    tmp["actual_dt"] = _combine_datetime(tmp, "date_dos", "Actual") if "Actual" in tmp.columns else pd.NaT
    tmp["a_depart_dt"] = _combine_datetime(tmp, "date_dos", "A_Depart") if "A_Depart" in tmp.columns else pd.NaT

    grp = ["date_dos", "cnr_tour", "nm_short_unload"]
    stops = (
        tmp.groupby(grp, as_index=False)
        .agg(
            rfx_activity=("RFX Activity", "first"),
            planned_dt=("planned_dt", "min"),
            actual_dt=("actual_dt", "min"),
            a_depart_dt=("a_depart_dt", "min"),
            planned=("Planned", "first"),
            actual=("Actual", "first"),
            a_depart=("A_Depart", "first") if "A_Depart" in tmp.columns else ("cnr_cust", "count"),
            durationP=("DurationP", "first") if "DurationP" in tmp.columns else ("cnr_cust", "count"),
            durationA=("DurationA", "first") if "DurationA" in tmp.columns else ("cnr_cust", "count"),
            orders=("cnr_cust", "count"),
        )
    )

    if "DurationP" in tmp.columns:
        stops["planned_block_min"] = pd.to_numeric(stops["durationP"], errors="coerce")
    else:
        stops["planned_block_min"] = pd.NA

    if "DurationA" in tmp.columns:
        stops["actual_block_min"] = pd.to_numeric(stops["durationA"], errors="coerce")
    else:
        stops["actual_block_min"] = pd.NA

    missing = stops["actual_block_min"].isna()
    if missing.any():
        fallback = (stops["a_depart_dt"] - stops["actual_dt"]).dt.total_seconds() / 60.0
        stops.loc[missing, "actual_block_min"] = fallback.loc[missing]

    stops["arrival_delta_min"] = (stops["actual_dt"] - stops["planned_dt"]).dt.total_seconds() / 60.0
    stops["delta_block_min"] = stops["actual_block_min"] - stops["planned_block_min"]

    stops["planned"] = stops["planned"].apply(_fmt_hhmm)
    stops["actual"] = stops["actual"].apply(_fmt_hhmm)
    stops["a_depart"] = stops["a_depart"].apply(_fmt_hhmm)

    s_pl = stops.sort_values(["planned_dt", "nm_short_unload"]).copy()
    s_pl["planned_pos"] = range(1, len(s_pl) + 1)

    s_ac = stops.sort_values(["actual_dt", "nm_short_unload"]).copy()
    s_ac["actual_pos"] = range(1, len(s_ac) + 1)

    out = s_pl.merge(
        s_ac[["date_dos", "cnr_tour", "nm_short_unload", "actual_pos"]],
        on=["date_dos", "cnr_tour", "nm_short_unload"],
        how="left",
    )
    out["seq_delta"] = out["actual_pos"] - out["planned_pos"]
    return out

@app.get("/transport_manager_html", response_class=HTMLResponse)
def transport_manager_html(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    rfx_activity: Optional[str] = Query(None),
    cnr_tour: Optional[str] = Query(None),
):
    df = load_orders(date_from=date_from, date_to=date_to, rfx_activity=rfx_activity, cnr_tour=cnr_tour)

    filter_html = f"""
    <form class="inline" method="get" action="/transport_manager_html">
      <label class="small">Datum van</label>
      <input type="date" name="date_from" value="{date_from or ''}">
      <label class="small">tot</label>
      <input type="date" name="date_to" value="{date_to or ''}">
      <label class="small">RFX Activity</label>
      <input type="text" name="rfx_activity" value="{rfx_activity or ''}" placeholder="bv. 4 of 5">
      <label class="small">Route</label>
      <input type="text" name="cnr_tour" value="{cnr_tour or ''}" placeholder="bv. 776907">
      <button type="submit" class="btn">Filter</button>
      <a href="/transport_manager_html" class="btn">Reset</a>
    </form>
    """

    if df.empty:
        body = f"""
        <h1>Transport manager analyse</h1>
        <p class="sub">Geen data binnen filters.</p>
        {filter_html}
        <p style="margin-top:14px"><a class="btn" href="/">⬅️ Dashboard</a></p>
        """
        return _layout("Transport analyse", body)

    rows = []
    for (d, tour), g in df.groupby(["date_dos", "cnr_tour"]):
        g = g.copy()
        stops = _stop_level_for_transport(g)

        dep_delay = _route_departure_delay(g)

        n_stops = int(stops["nm_short_unload"].nunique())
        mism = int((stops["seq_delta"].fillna(0) != 0).sum())
        max_abs = float(stops["seq_delta"].abs().max()) if len(stops) else 0.0

        planned_total = float(pd.to_numeric(stops["planned_block_min"], errors="coerce").fillna(0).sum())
        actual_total = float(pd.to_numeric(stops["actual_block_min"], errors="coerce").fillna(0).sum())
        delta_total = actual_total - planned_total

        rows.append(
            {
                "date_dos": str(d),
                "cnr_tour": str(tour),
                "rfx_activity": str(g["RFX Activity"].iloc[0]) if "RFX Activity" in g.columns else "",
                "leverpunten": n_stops,
                "seq_mismatch_cnt": mism,
                "max_abs_seq_delta": int(max_abs) if pd.notna(max_abs) else 0,
                "dep_delay_min": dep_delay,
                "planned_block_total_min": planned_total,
                "actual_block_total_min": actual_total,
                "delta_block_total_min": delta_total,
            }
        )

    out = pd.DataFrame(rows).sort_values(["date_dos", "delta_block_total_min"], ascending=[True, False])

    headers = """
      <th>Datum</th>
      <th>Route</th>
      <th>RFX Activity</th>
      <th># Leverpunten</th>
      <th># Seq mismatch</th>
      <th>Max |seq delta|</th>
      <th>Vertrek delay (min)</th>
      <th>Planned block total (min)</th>
      <th>Actual block total (min)</th>
      <th>Delta block total (min)</th>
      <th>Detail</th>
    """
    rows_html = ""
    for _, r in out.iterrows():
        link = f"/transport_route_detail_html?date={r['date_dos']}&cnr_tour={r['cnr_tour']}"
        dep = "" if pd.isna(r["dep_delay_min"]) else f"{float(r['dep_delay_min']):.0f}"
        rows_html += f"""
        <tr>
          <td class="mono">{r['date_dos']}</td>
          <td class="mono">{r['cnr_tour']}</td>
          <td class="mono">{r.get('rfx_activity','')}</td>
          <td class="mono">{int(r['leverpunten'])}</td>
          <td class="mono">{int(r['seq_mismatch_cnt'])}</td>
          <td class="mono">{int(r['max_abs_seq_delta'])}</td>
          <td class="mono">{dep}</td>
          <td class="mono">{float(r['planned_block_total_min']):.0f}</td>
          <td class="mono">{float(r['actual_block_total_min']):.0f}</td>
          <td class="mono">{float(r['delta_block_total_min']):.0f}</td>
          <td><a class="btn" href="{link}">🔍 Detail</a></td>
        </tr>
        """

    table_html = f"""
    <div class="topbar">
      <div>
        <h3 style="margin:0">Overzicht per route</h3>
        <div class="sub">
          • Seq mismatch: aantal leverpunten waar volgorde afwijkt (actual_pos ≠ planned_pos).<br/>
          • Vertrek delay = A_Depart − P_Depart (min).<br/>
          • Block total = som(DurationP) vs som(DurationA of A_Depart−Actual) over leverpunten.
        </div>
      </div>
      <button class="copy-btn" onclick="copyTable('tblTransportRoutes')">📋 Kopieer tabel</button>
    </div>
    <div class="table-wrapper">
      <table id="tblTransportRoutes">
        <thead><tr>{headers}</tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
    """

    body = f"""
    <h1>Transport manager analyse</h1>
    <p class="sub">
      1) Volgorde drops vs planning (sequence delta).<br/>
      2) Reëel vertrek vs gepland vertrek.<br/>
      3) Planned (DurationP) + wachttijd vs reëel (DurationA of A_Depart−Actual).
    </p>
    {filter_html}
    <br/>
    {table_html}
    <p style="margin-top:14px"><a class="btn" href="/">⬅️ Dashboard</a></p>
    """
    return _layout("Transport analyse", body)

@app.get("/transport_route_detail_html", response_class=HTMLResponse)
def transport_route_detail_html(date: str, cnr_tour: str):
    df = load_orders(date_from=date, date_to=date, cnr_tour=cnr_tour)
    if df.empty:
        return _layout("Transport route detail", f"<h1>Transport detail</h1><p class='sub'>Geen data voor {date} / route {cnr_tour}</p>")

    stops = _stop_level_for_transport(df)
    dep_delay = _route_departure_delay(df)

    # TABEL 1: Sequence
    seq = stops.copy().sort_values(["planned_pos"])
    headers1 = """
      <th>Leverpunt</th>
      <th>Planned</th>
      <th>Actual</th>
      <th>Planned pos</th>
      <th>Actual pos</th>
      <th>Seq delta</th>
      <th>Orders</th>
      <th>Route detail</th>
    """
    rows1 = ""
    for _, r in seq.iterrows():
        link = f"/route_detail_html?date={date}&cnr_tour={cnr_tour}&view=delivery"
        rows1 += f"""
        <tr>
          <td>{r['nm_short_unload']}</td>
          <td class="mono">{r.get('planned','')}</td>
          <td class="mono">{r.get('actual','')}</td>
          <td class="mono">{int(r.get('planned_pos',0))}</td>
          <td class="mono">{int(r.get('actual_pos',0))}</td>
          <td class="mono">{int(r.get('seq_delta',0))}</td>
          <td class="mono">{int(r.get('orders',0))}</td>
          <td><a class="btn" href="{link}">🔍 Route</a></td>
        </tr>
        """

    table1 = f"""
    <div class="topbar">
      <div>
        <h3 style="margin:0">1) Volgorde drops vs planning</h3>
        <div class="sub">Seq delta = actual_pos − planned_pos (positief = later uitgevoerd dan gepland).</div>
      </div>
      <button class="copy-btn" onclick="copyTable('tblSeq')">📋 Kopieer tabel</button>
    </div>
    <div class="table-wrapper">
      <table id="tblSeq">
        <thead><tr>{headers1}</tr></thead>
        <tbody>{rows1}</tbody>
      </table>
    </div>
    """

    # TABEL 2: Departure
    dep_val = "" if pd.isna(dep_delay) else f"{float(dep_delay):.0f}"
    table2 = f"""
    <div class="topbar">
      <div>
        <h3 style="margin:0">2) Reëel vertrek vs gepland vertrek</h3>
        <div class="sub">Vertrek delay (min) = A_Depart − P_Depart (route-level).</div>
      </div>
      <button class="copy-btn" onclick="copyTable('tblDep')">📋 Kopieer tabel</button>
    </div>
    <div class="table-wrapper">
      <table id="tblDep">
        <thead>
          <tr>
            <th>Datum</th><th>Route</th><th>Vertrek delay (min)</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td class="mono">{date}</td>
            <td class="mono">{cnr_tour}</td>
            <td class="mono">{dep_val}</td>
          </tr>
        </tbody>
      </table>
    </div>
    """

    # TABEL 3: Planned+wait vs actual+wait (block)
    blk = stops.copy().sort_values(["delta_block_min"], ascending=False)
    headers3 = """
      <th>Leverpunt</th>
      <th>Planned</th>
      <th>Actual</th>
      <th>A_Depart</th>
      <th>Planned block (min)</th>
      <th>Actual block (min)</th>
      <th>Delta block (min)</th>
      <th>Arrival delta (min)</th>
      <th>Orders</th>
    """
    rows3 = ""
    for _, r in blk.iterrows():
        pb = "" if pd.isna(r["planned_block_min"]) else f"{float(r['planned_block_min']):.0f}"
        ab = "" if pd.isna(r["actual_block_min"]) else f"{float(r['actual_block_min']):.0f}"
        db = "" if pd.isna(r["delta_block_min"]) else f"{float(r['delta_block_min']):.0f}"
        ad = "" if pd.isna(r["arrival_delta_min"]) else f"{float(r['arrival_delta_min']):.0f}"
        rows3 += f"""
        <tr>
          <td>{r['nm_short_unload']}</td>
          <td class="mono">{r.get('planned','')}</td>
          <td class="mono">{r.get('actual','')}</td>
          <td class="mono">{r.get('a_depart','')}</td>
          <td class="mono">{pb}</td>
          <td class="mono">{ab}</td>
          <td class="mono">{db}</td>
          <td class="mono">{ad}</td>
          <td class="mono">{int(r.get('orders',0))}</td>
        </tr>
        """

    table3 = f"""
    <div class="topbar">
      <div>
        <h3 style="margin:0">3) Geplande levertijd+wachttijd vs reële levertijd en wachttijd</h3>
        <div class="sub">
          Planned block = DurationP (min). Actual block = DurationA (min) of fallback (A_Depart−Actual).<br/>
          Arrival delta = Actual − Planned (min) helpt scheiden transport vs site-effect.
        </div>
      </div>
      <button class="copy-btn" onclick="copyTable('tblBlock')">📋 Kopieer tabel</button>
    </div>
    <div class="table-wrapper">
      <table id="tblBlock">
        <thead><tr>{headers3}</tr></thead>
        <tbody>{rows3}</tbody>
      </table>
    </div>
    """

    body = f"""
    <h1>Transport detail – route {cnr_tour} op {date}</h1>
    <p class="sub">Detailpagina met 3 tabellen (allemaal kopieerbaar).</p>

    <p>
      <a class="btn" href="/transport_manager_html">⬅️ Terug naar transport overzicht</a>
      &nbsp;
      <a class="btn" href="/route_detail_html?date={date}&cnr_tour={cnr_tour}&view=delivery">🚛 Route detail (JIT)</a>
      &nbsp;
      <a class="btn" href="/">🏠 Dashboard</a>
    </p>

    {table1}
    <br/>
    {table2}
    <br/>
    {table3}
    """
    return _layout("Transport route detail", body)