# app_streamlit.py
import os
import subprocess
from pathlib import Path
import io
import urllib.parse
import datetime
from dateutil.relativedelta import relativedelta
from typing import Dict, List, Optional, Tuple
import importlib
import re, sys

import streamlit as st
import pandas as pd
import requests
import plotly.express as px
import plotly.graph_objects as go

# --- auto-launch for convenience (VSCode "Run Python File") ---
def init_auto_launch():
    if os.environ.get("STREAMLIT_RUN") != "1":
        os.environ["STREAMLIT_RUN"] = "1"
        script_path = str(Path(__file__).resolve())
        subprocess.run([sys.executable, "-m", "streamlit", "run", script_path], check=False)
        raise SystemExit
init_auto_launch()

# -----------------------
# Embedded FundISIN mapping (edit if needed)
# -----------------------
FundISIN = pd.DataFrame({
    "Nom": [
        "AMUNDI S.F. - DIVERSIFIED SHORT-TERM BOND SELECT - A EUR (C)",
        "AMUNDI S.F. - DIVERSIFIED SHORT-TERM BOND SELECT - A EUR (C)",
        "AMUNDI S.F. - DIVERSIFIED SHORT-TERM BOND SELECT - I EUR (C)",
        "AMUNDI CREDIT EURO - I",
        "AMUNDI CREDIT EURO - I2",
        "AMUNDI FUNDS EURO CORPORATE BOND SELECT - I EUR",
        "AMUNDI FUNDS EURO CORPORATE BOND SELECT - A EUR (C)"
    ],
    "ISIN": [
        "LU1706854152","LU2357810188","LU1706854400",
        "FR0000446288","FR0010628644","LU0119099496","LU0119099819"
    ]
})

AUTO_VIEWS = [("Historique VL Base 100", "HistoBase100"), ("Indicateur risque", "IndicRisque")]

# -----------------------
# Helpers
# -----------------------
def clean_text(s: Optional[str]) -> str:
    if s is None:
        return ""
    return " ".join(str(s).split()).strip()

def build_label_column(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["Nom"] = out["Nom"].astype(str).apply(clean_text)
    out["ISIN"] = out["ISIN"].astype(str).apply(clean_text)
    out["Label"] = out.apply(lambda r: f"{r['Nom']} — {r['ISIN']}", axis=1)
    return out.reset_index(drop=True)

def last_day_previous_month(ref: Optional[datetime.date] = None) -> datetime.date:
    if ref is None:
        ref = datetime.date.today()
    first_of_month = ref.replace(day=1)
    return first_of_month - datetime.timedelta(days=1)

def build_url(template: str, isin_for_url: str, viewkey: str, date_iso: str) -> str:
    nom_enc = urllib.parse.quote_plus(str(isin_for_url))
    key_enc = urllib.parse.quote_plus(str(viewkey))
    url = template.replace("{Nom}", nom_enc).replace("{Type}", key_enc)
    if not date_iso:
        return url
    if url.endswith(("=", "&", "?", "date=")):
        return url + urllib.parse.quote_plus(date_iso)
    if "date=" in url:
        return url + "&date=" + urllib.parse.quote_plus(date_iso)
    sep = "&" if "?" in url else "?"
    return url + f"{sep}date=" + urllib.parse.quote_plus(date_iso)

def parse_value_to_number(x):
    if pd.isna(x):
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if s == "":
        return None
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]
    s = s.replace("\u00A0", "").replace("\u2009", "").replace(" ", "")
    if s.count(",") > 0 and s.count(".") == 0:
        s = s.replace(",", ".")
    else:
        s = s.replace(",", "")
    if s.endswith("%"):
        try:
            val = float(s[:-1]) / 100.0
            return -val if neg else val
        except:
            return None
    try:
        val = float(s)
        return -val if neg else val
    except:
        return None

def df_to_excel_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    buf.seek(0)
    return buf.getvalue()

def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")

def sanitize_colname(c):
    if c is None:
        return ""
    s = str(c).strip()
    s = s.replace(":", " -").replace("/", " /").replace("\\", " ")
    s = " ".join(s.split())
    return s

def sanitize_and_uniq_cols(df: pd.DataFrame) -> pd.DataFrame:
    cols = list(df.columns)
    new_cols = []
    counts = {}
    for c in cols:
        base = sanitize_colname(c)
        if base == "":
            base = "col"
        if base in counts:
            counts[base] += 1
            new = f"{base}__{counts[base]}"
        else:
            counts[base] = 0
            new = base
        new_cols.append(new)
    df2 = df.copy()
    df2.columns = new_cols
    return df2

def detect_date_col(df: pd.DataFrame) -> Optional[str]:
    for c in df.columns:
        try:
            if isinstance(c, str) and "date" in c.lower():
                return c
        except Exception:
            continue
    for candidate in ["Date de la perf", "Date de perf", "Date"]:
        if candidate in df.columns:
            return candidate
    return None

def extract_date_series(df: pd.DataFrame, date_col_name: str) -> pd.Series:
    matching_cols = [c for c in df.columns if str(c) == str(date_col_name)]
    if not matching_cols:
        for c in df.columns:
            if isinstance(c, str) and "date" in c.lower():
                matching_cols = [c]
                break
    if not matching_cols:
        return pd.Series([pd.NaT] * len(df), index=df.index)
    if len(matching_cols) == 1:
        s = df[matching_cols[0]]
    else:
        sub = df.loc[:, matching_cols]
        s = sub.bfill(axis=1).iloc[:, 0]
    return pd.to_datetime(s, dayfirst=True, errors="coerce")

def read_html_safe(html_text: str):
    try:
        return pd.read_html(html_text)
    except Exception:
        try:
            return pd.read_html(html_text, flavor="bs4")
        except Exception:
            return []

# -----------------------
# Cache-compatible HTTP fetch
# -----------------------
try:
    cache_fetch = st.cache_data
except Exception:
    cache_fetch = st.cache

@cache_fetch
def fetch_url_content(url: str, verify_ssl: bool, timeout: int = 30) -> Optional[str]:
    try:
        r = requests.get(url, verify=verify_ssl, timeout=timeout)
        r.raise_for_status()
        return r.text
    except Exception:
        return None

# -----------------------
# Sidebar (minimal)
# -----------------------
def sidebar_inputs():
    st.sidebar.header("Paramètres")
    template = st.sidebar.text_input(
        "Base URL template",
        value="https://alto.intramundi.com/reporting/client-reporting/designer/render/excelView?viewKey={Type}&occurence={Nom}&date="
    )
    verify_ssl = st.sidebar.checkbox("Vérifier SSL (verify=True)", value=False)
    debug = st.sidebar.checkbox("URL check (debug)", value=False)
    return {"template": template, "verify_ssl": verify_ssl, "debug": debug}

# -----------------------
# Fetch both views
# -----------------------
def fetch_auto_views(template: str, isin: str, asof: datetime.date, verify_ssl: bool, debug: bool):
    results: Dict[str, Dict[str, pd.DataFrame]] = {}
    diagnostics: List[Dict[str, str]] = []
    for label, vk in AUTO_VIEWS:
        results[vk] = {}
        date_iso = asof.isoformat()
        url = build_url(template, isin, vk, date_iso)
        if debug:
            diagnostics.append({"view": vk, "url": url})
        html = fetch_url_content(url, verify_ssl)
        if html:
            tables = read_html_safe(html)
            if tables:
                results[vk][date_iso] = tables[0].drop_duplicates()
    return results, diagnostics

# -----------------------
# Combine helper
# -----------------------
def combine_results_by_view(results_by_view: Dict[str, Dict[str, pd.DataFrame]]) -> Dict[str, pd.DataFrame]:
    combined: Dict[str, pd.DataFrame] = {}
    for vk, mapping in results_by_view.items():
        if not mapping:
            combined[vk] = pd.DataFrame()
            continue
        frames = []
        for date_str, df in mapping.items():
            tmp = df.copy()
            tmp["Date"] = date_str
            frames.append(tmp)
        combined[vk] = pd.concat(frames, ignore_index=True)
    return combined

# -----------------------
# Raw data preview
# -----------------------
def raw_preview_tab(results_by_view: Dict[str, Dict[str, pd.DataFrame]]):
    st.header("Données brutes")
    if not results_by_view:
        st.info("Aucune donnée.")
        return
    for vk, mapping in results_by_view.items():
        with st.expander(f"{vk} — snapshots: {len(mapping)}", expanded=False):
            if not mapping:
                st.write("Aucune table.")
                continue
            dates = sorted(mapping.keys())
            sel = st.selectbox(f"{vk} snapshot", options=dates, key=f"raw_{vk}")
            df = mapping.get(sel)
            if df is None or df.empty:
                st.write("Vide")
            else:
                st.write("Shape:", df.shape)
                st.dataframe(df.head(200))
                st.download_button(f"Télécharger {vk} ({sel})", data=df_to_excel_bytes(df), file_name=f"{vk}_{sel}.xlsx")

# -----------------------
# Performance helpers & panel
# -----------------------
def compute_period_return(series: pd.Series, start: pd.Timestamp, end: pd.Timestamp) -> Optional[float]:
    if series is None or series.empty:
        return None
    try:
        a = series.asof(start)
        b = series.asof(end)
    except Exception:
        return None
    if a is None or b is None or a == 0:
        return None
    return (b / a) - 1.0

def annualize(cum_return: float, days: int) -> Optional[float]:
    if cum_return is None or days <= 0:
        return None
    years = days / 365.25
    try:
        return (1 + cum_return) ** (1 / years) - 1.0
    except Exception:
        return None

def format_trim(x, decimals=6):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return "N/A"
    s = f"{x:.{decimals}f}"
    s = s.rstrip("0").rstrip(".")
    return s

def process_performance_net(df_combined: pd.DataFrame, fund_name: str):
    st.header(fund_name)
    if df_combined is None or df_combined.empty:
        st.info("Aucune donnée HistoBase100.")
        return

    df = sanitize_and_uniq_cols(df_combined)
    date_col = detect_date_col(df)
    if date_col is None:
        st.dataframe(df.head(200))
        return
    df["Date"] = extract_date_series(df, date_col)
    df = df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)

    # detect key cols
    perf_col = None; bench_col = None; actif_col = None; nav_col = None
    for c in df.columns:
        lc = str(c).lower()
        if "nav" in lc and nav_col is None: nav_col = c
        if "actif" in lc and "net" in lc and actif_col is None: actif_col = c
        if "benchmark" in lc or "bench" in lc: bench_col = bench_col or c
        if "perf" in lc and "vm" in lc: perf_col = perf_col or c
    if perf_col is None:
        candidates = [c for c in df.columns if c != "Date"]
        perf_col = candidates[0] if candidates else None

    candidate_cols = [c for c in [perf_col, bench_col, actif_col, nav_col] if c and c in df.columns]
    for c in candidate_cols:
        df[c + "_val"] = df[c].apply(parse_value_to_number).astype("float64")
    val_cols = [c + "_val" for c in candidate_cols]
    if val_cols:
        df[val_cols] = df[val_cols].bfill()

    # date inputs (start above end) with Since inception button
    min_date = df["Date"].min().date(); max_date = df["Date"].max().date()
    st.write(f"Période disponible : {min_date} — {max_date}")

    if st.button("Since inception"):
        st.session_state["perf_start_v"] = min_date

    start_date = st.date_input("Date de début", value=st.session_state.get("perf_start_v", min_date), min_value=min_date, max_value=max_date, key="perf_start_v")
    end_date = st.date_input("Date de fin", value=max_date, min_value=min_date, max_value=max_date, key="perf_end_v")
    if start_date > end_date:
        st.error("Date début > date fin"); return

    sub = df[(df["Date"].dt.date >= start_date) & (df["Date"].dt.date <= end_date)].copy()
    if sub.empty:
        st.info("Aucune donnée sur la plage choisie."); return

    perf_series = sub.set_index("Date")[perf_col + "_val"] if perf_col and (perf_col + "_val") in sub.columns else None
    bench_series = sub.set_index("Date")[bench_col + "_val"] if bench_col and (bench_col + "_val") in sub.columns else None
    actif_series = sub.set_index("Date")[actif_col + "_val"] if actif_col and (actif_col + "_val") in sub.columns else None
    nav_series = sub.set_index("Date")[nav_col + "_val"] if nav_col and (nav_col + "_val") in sub.columns else None

    # Performance over selected period (non-annualized)
    start_ts = pd.to_datetime(start_date)
    end_ts = pd.to_datetime(end_date)
    perf_period_f = compute_period_return(perf_series, start_ts, end_ts) if perf_series is not None else None
    perf_period_b = compute_period_return(bench_series, start_ts, end_ts) if bench_series is not None else None
    k1, k2 = st.columns(2)
    k1.metric("Perf période", f"{(perf_period_f*100):.2f}%" if perf_period_f is not None else "N/A")
    k2.metric("Perf période — Benchmark", f"{(perf_period_b*100):.2f}%" if perf_period_b is not None else "N/A")
    last_ts = pd.to_datetime(end_date)  # use arrival date per request

    # Performance graph (full width)
    st.caption("Performance (Base 100)")
    if perf_series is not None:
        dfp = pd.DataFrame({"fund": perf_series})
        if bench_series is not None:
            dfp["bench"] = bench_series
        dfp = dfp.dropna(how="all").ffill()
        if not dfp.empty:
            df_rb = dfp.apply(lambda s: (s / s.iloc[0]) * 100 if (s.notna().any() and s.iloc[0] and s.iloc[0] != 0) else s)
            fig = go.Figure()
            if "fund" in df_rb.columns:
                fig.add_trace(go.Scatter(x=df_rb.index, y=df_rb["fund"], mode="lines", name=fund_name))
            if "bench" in df_rb.columns:
                fig.add_trace(go.Scatter(x=df_rb.index, y=df_rb["bench"], mode="lines", name="Benchmark"))
            fig.update_layout(template="plotly_white", hovermode="x unified", margin=dict(t=30))
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.write("Pas de série de performance disponible.")
    
    def asof_get(s, d):
        try: return s.asof(d)
        except: return None

    nav_last = asof_get(nav_series, last_ts) if nav_series is not None else None
    actif_last = asof_get(actif_series, last_ts) if actif_series is not None else None

    cnav, cact = st.columns(2)
    cnav.metric("NAV au "+ str(end_date), format_trim(nav_last, decimals=6) if nav_last is not None else "N/A")
    cact.metric("Actif net (M€)", f"{(actif_last/1_000_000):,.2f} M" if actif_last is not None else "N/A")
    
    # NAV & Actif graphs (side by side)
    cols_nav_act = st.columns(2)
    with cols_nav_act[0]:
        st.caption("NAV (M€)")
        if nav_series is not None:
            dfn = nav_series.reset_index().rename(columns={nav_col + "_val": "NAV"})
            dfn["NAV (M)"] = dfn["NAV"] / 1_000_000.0
            fig_nav = px.line(dfn, x="Date", y="NAV (M)", template="plotly_white")
            st.plotly_chart(fig_nav, use_container_width=True)
        else:
            st.write("Pas de NAV disponible.")
    with cols_nav_act[1]:
        st.caption("Actif net (M€)")
        if actif_series is not None:
            df_act = actif_series.reset_index().rename(columns={actif_col + "_val": "Actif net"})
            df_act["Actif net (M)"] = df_act["Actif net"] / 1_000_000.0
            fig_act = px.line(df_act, x="Date", y="Actif net (M)", template="plotly_white")
            st.plotly_chart(fig_act, use_container_width=True)
        else:
            st.write("Pas d'actif net disponible.")



    
    # multi-period summary and annualized (kept unchanged)
    periods_list = [
        ("YTD", None), ("1M", {"months": 1}), ("3M", {"months": 3}), ("6M", {"months": 6}),
        ("1Y", {"years": 1}), ("3Y", {"years": 3}), ("5Y", {"years": 5}), ("7Y", {"years": 7}), ("10Y", {"years": 10}), ("Since", "since")
    ]
    rows = []
    ann_rows = []
    for label, delta in periods_list:
        if label == "YTD":
            s_ts = pd.to_datetime(datetime.date(last_ts.year, 1, 1))
        elif delta == "since":
            s_ts = sub["Date"].min()
        else:
            if "months" in delta:
                s_ts = last_ts - relativedelta(months=delta["months"])
            else:
                s_ts = last_ts - relativedelta(years=delta["years"])
        vf = compute_period_return(perf_series, s_ts, last_ts)
        vb = compute_period_return(bench_series, s_ts, last_ts) if bench_series is not None else None
        days = (last_ts - pd.to_datetime(s_ts)).days
        vf_ann = annualize(vf, days) if vf is not None and days > 0 else None
        vb_ann = annualize(vb, days) if vb is not None and days > 0 else None
        rows.append({"Period": label, "PTF": vf, "Benchmark": vb})
        ann_rows.append({"Period": label, "PTF_ann": vf_ann, "Bench_ann": vb_ann})

    summary_df = pd.DataFrame(rows).set_index("Period")
    summary_fmt = summary_df.copy()
    for c in ["PTF", "Benchmark"]:
        if c in summary_fmt.columns:
            summary_fmt[c] = summary_fmt[c].apply(lambda x: f"{x*100:.2f}%" if x is not None else "")

    st.subheader("Synthèse (cumulée)")
    st.dataframe(summary_fmt)
    st.download_button("Télécharger synthèse (Excel)", data=df_to_excel_bytes(summary_df.reset_index()), file_name="synthese.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    ann_df = pd.DataFrame(ann_rows).set_index("Period")
    ann_fmt = ann_df.copy()
    for c in ann_fmt.columns:
        ann_fmt[c] = ann_fmt[c].apply(lambda x: f"{x*100:.2f}%" if x is not None else "")
    st.subheader("Synthèse (annualisée)")
    st.dataframe(ann_fmt)
    st.download_button("Télécharger annualisé (Excel)", data=df_to_excel_bytes(ann_df.reset_index()), file_name="annualized.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # performance per year (base complete)
    st.subheader("Performance par année (base complète)")
    yearly_full = df.copy()
    yearly_full["Year"] = yearly_full["Date"].dt.year
    years_all = sorted(yearly_full["Year"].unique(), reverse=True)
    ann_rows_full = []
    for y in years_all:
        y_df = yearly_full[yearly_full["Year"] == y].sort_values("Date")
        if y_df.empty:
            continue
        first_row = y_df.iloc[0]
        last_row = y_df.iloc[-1]
        vm_first = first_row.get(perf_col + "_val") if (perf_col and (perf_col + "_val") in y_df.columns) else None
        vm_last = last_row.get(perf_col + "_val") if (perf_col and (perf_col + "_val") in y_df.columns) else None
        bench_first_y = first_row.get(bench_col + "_val") if (bench_col and (bench_col + "_val") in y_df.columns) else None
        bench_last_y = last_row.get(bench_col + "_val") if (bench_col and (bench_col + "_val") in y_df.columns) else None
        vm_ret = (vm_last / vm_first - 1.0) if (vm_first and vm_last and vm_first != 0) else None
        bench_ret = (bench_last_y / bench_first_y - 1.0) if (bench_first_y and bench_last_y and bench_first_y != 0) else None
        ann_rows_full.append({"Année": y, fund_name: vm_ret, "Benchmark": bench_ret})
    ann_df_full = pd.DataFrame(ann_rows_full).set_index("Année")
    ann_df_fmt = ann_df_full.copy()
    for c in ann_df_fmt.columns:
        ann_df_fmt[c] = ann_df_fmt[c].apply(lambda x: f"{x*100:.2f}%" if (x is not None and not pd.isna(x)) else "")
    col_a, col_b = st.columns([4,1])
    with col_b:
        st.download_button("Télécharger perf par année (Excel)", data=df_to_excel_bytes(ann_df_full.reset_index()), file_name="perf_par_annee.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    with col_a:
        st.dataframe(ann_df_fmt)

# -----------------------
# Risk panel
# -----------------------
def process_risk_analysis(results_by_view: Dict[str, Dict[str, pd.DataFrame]]):
    st.header("Risk analysis — indicateurs")
    vk = "IndicRisque"
    mapping = results_by_view.get(vk, {})
    if not mapping:
        st.info("Aucune donnée IndicRisque.")
        return
    keys = sorted(mapping.keys())
    latest = keys[-1]
    df_raw = mapping[latest]
    if df_raw is None or df_raw.empty:
        st.info("Snapshot vide.")
        return
    df = sanitize_and_uniq_cols(df_raw.copy())
    date_col = detect_date_col(df)
    if date_col:
        df["Date"] = extract_date_series(df, date_col)
    else:
        df["Date"] = pd.to_datetime(latest)
    id_col = df.columns[0]
    durations = [c for c in df.columns if c not in [id_col, "Date"]]
    if not durations:
        st.info("Aucune durée détectée.")
        return
    df_melt = df[[id_col] + durations].melt(id_vars=[id_col], var_name="Duration", value_name="RawValue")
    df_melt["Value"] = df_melt["RawValue"].apply(parse_value_to_number)

    def extract_series_and_base(s: str):
        s0 = str(s)
        s_low = s0.lower()
        series = None
        if re.search(r"\b(ptf|portefeuille)\b", s_low):
            series = "PTF"
        elif re.search(r"\b(bench|benchmark)\b", s_low):
            series = "BENCH"
        base = re.sub(r"\b(valeur ptf|valeur bench|valeur|ptf|portefeuille|bench|benchmark)\b", "", s0, flags=re.I)
        base = re.sub(r"[_\:\-\(\)]", " ", base).strip()
        base = " ".join(base.split())
        if base == "":
            base = s0
        return series, base

    ex = df_melt[id_col].astype(str).apply(lambda x: extract_series_and_base(x))
    df_melt["Series"] = ex.apply(lambda t: t[0])
    df_melt["Metric"] = ex.apply(lambda t: t[1])
    df_melt["Series"] = df_melt["Series"].fillna(df_melt[id_col].astype(str).apply(lambda s: ("PTF" if re.search(r"ptf", s, re.I) else ("BENCH" if re.search(r"bench|benchmark", s, re.I) else None))))
    tidy = df_melt[["Metric", "Series", "Duration", "RawValue", "Value"]].copy()

    def duration_to_months(d: str) -> int:
        s = str(d).lower()
        m = re.search(r"(\d+)\s*mois", s)
        if m:
            return int(m.group(1))
        y = re.search(r"(\d+)\s*an", s)
        if y:
            return int(y.group(1)) * 12
        if "depuis" in s:
            return 10 ** 9
        return 10 ** 6

    durations_sorted = sorted(list(tidy["Duration"].unique()), key=lambda x: duration_to_months(x))

    st.subheader("Filtres")
    metrics = sorted(tidy["Metric"].unique().tolist())
    sel_metric = st.selectbox("Metric", options=metrics, index=0)
    sel_duration = st.selectbox("Durée", options=durations_sorted, index=0)
    show_comp = st.checkbox("Afficher comparatif", value=True)

    sub_tidy = tidy[(tidy["Metric"] == sel_metric) & (tidy["Duration"] == sel_duration)].copy()
    if sub_tidy.empty:
        sm = str(sel_metric).lower()
        sd = str(sel_duration).lower()
        sub_tidy = tidy[
            tidy["Metric"].astype(str).str.lower().str.contains(sm, na=False) &
            tidy["Duration"].astype(str).str.lower().str.contains(sd, na=False)
        ].copy()
    if sub_tidy.empty:
        st.warning("Aucune correspondance stricte — affichage pour diagnostic")
        inspect = tidy[tidy["Duration"].astype(str).str.lower().str.contains(str(sel_duration).lower(), na=False)].copy()
        inspect["ValueParsed"] = inspect["RawValue"].apply(parse_value_to_number)
        st.dataframe(inspect[["Metric", "Series", "Duration", "RawValue", "ValueParsed"]].head(200))
        return

    metric_is_percent = str(sel_metric).strip().startswith("%")

    def normalize(metric_label: str, raw_value):
        if raw_value is None:
            return None
        try:
            vv = float(raw_value)
        except:
            return None
        if str(metric_label).strip().startswith("%"):
            if abs(vv) > 1.0:
                return vv / 100.0
            else:
                return vv
        return vv

    sub_tidy["ValueNorm"] = sub_tidy.apply(lambda r: normalize(sel_metric, r["Value"]), axis=1)
    pivot_display = sub_tidy.pivot_table(index=None, columns="Series", values="ValueNorm", aggfunc="first")
    st.dataframe(pivot_display)

    special_case = (str(sel_metric).strip().lower() == "% month down") and ("depuis" in str(sel_duration).lower())
    if show_comp and not special_case:
        long_plot = pivot_display.reset_index(drop=True).melt(var_name="Series", value_name="Value").dropna(subset=["Value"])
        if not long_plot.empty:
            if metric_is_percent:
                long_plot["Display"] = long_plot["Value"].apply(lambda x: f"{x*100:.2f}%" if x is not None else "")
                fig = px.bar(long_plot, x="Series", y="Value", color="Series", text="Display")
                fig.update_layout(yaxis_tickformat=",.2%")
            else:
                long_plot["Display"] = long_plot["Value"].apply(lambda x: f"{x:.4g}" if x is not None else "")
                fig = px.bar(long_plot, x="Series", y="Value", color="Series", text="Display")
            st.plotly_chart(fig, use_container_width=True)
    else:
        if special_case:
            st.info("Comparatif désactivé pour cette combination.")

    full_pivot = tidy.pivot_table(index=["Metric", "Series"], columns="Duration", values="Value", aggfunc="first")
    cols_current = list(full_pivot.columns)
    ordered_cols = [c for c in durations_sorted if c in cols_current]
    remaining = [c for c in cols_current if c not in ordered_cols]
    final_cols = ordered_cols + remaining
    full_pivot = full_pivot.reindex(columns=final_cols)
    st.subheader("Tableau complet — durées triées")
    st.dataframe(full_pivot)

    st.download_button("Télécharger IndicRisque (Excel)", data=df_to_excel_bytes(tidy), file_name="IndicRisque_tidy.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# -----------------------
# top controls (styled)
# -----------------------
def top_controls(fundisin_df: pd.DataFrame):
    st.markdown("<div style='border:1px solid #e6e9ee; padding:12px; border-radius:10px; background:linear-gradient(180deg,#ffffff,#fbfdff);'>", unsafe_allow_html=True)
    cols = st.columns([3,3,2])
    funds_df = build_label_column(fundisin_df[['Nom', 'ISIN']])
    fund_names = sorted(funds_df['Nom'].unique())
    with cols[0]:
        selected_name_top = st.selectbox("Nom du fonds", options=fund_names, key="top_fund_name")
    isins_for_name = funds_df.loc[funds_df['Nom'] == selected_name_top, 'ISIN'].tolist()
    with cols[1]:
        if not isins_for_name:
            selected_isin_top = None
            st.error("Aucun ISIN trouvé pour ce nom.")
        elif len(isins_for_name) == 1:
            selected_isin_top = isins_for_name[0]
            st.text_input("Part (ISIN)", value=selected_isin_top, disabled=True, key="top_single_isin_display")
        else:
            selected_isin_top = st.selectbox("Part (ISIN)", options=isins_for_name, key="top_isin")
    with cols[2]:
        default_asof = last_day_previous_month()
        selected_asof_top = st.date_input("Date (as‑of)", value=default_asof, key="top_asof")
    st.markdown("</div>", unsafe_allow_html=True)
    return {"selected_name": selected_name_top, "selected_isin": selected_isin_top, "asof": selected_asof_top}

# -----------------------
# main
# -----------------------
def main():
    st.set_page_config(page_title="Time Series Extractor — Dashboard", layout="wide")
    st.title("Time Series Extractor — Dashboard")
    st.markdown("Choisis le fonds et la date ; HistoBase100 + IndicRisque seront chargés automatiquement.")
    sidebar_params = sidebar_inputs()
    top_params = top_controls(FundISIN)
    st.write({"Nom": top_params.get("selected_name"), "ISIN": top_params.get("selected_isin"), "Date as‑of": str(top_params.get("asof"))})
    last = st.session_state.get("last_fetch_params", {})
    current = {"isin": top_params.get("selected_isin"), "asof": str(top_params.get("asof"))}
    do_fetch = False
    if current["isin"] and current["asof"]:
        if last.get("isin") != current["isin"] or last.get("asof") != current["asof"]:
            do_fetch = True
    if st.button("Forcer rechargement"):
        do_fetch = True
    if do_fetch:
        if not current["isin"]:
            st.error("Sélectionne une part (ISIN) avant le chargement.")
        else:
            with st.spinner("Chargement des vues..."):
                results, diags = fetch_auto_views(sidebar_params["template"], current["isin"], datetime.date.fromisoformat(current["asof"]), sidebar_params["verify_ssl"], sidebar_params["debug"])
                st.session_state["results_by_view"] = results
                st.session_state["combined_by_view"] = combine_results_by_view(results)
                st.session_state["last_fetch_params"] = current
                if sidebar_params["debug"]:
                    st.markdown("### DEBUG URLs")
                    for d in diags:
                        st.write(d)
    results_by_view = st.session_state.get("results_by_view", {})
    if results_by_view:
        tabs = st.tabs(["Performance net", "Risk analysis", "Raw data"])
        with tabs[0]:
            dfh = results_by_view.get("HistoBase100", {})
            combined_h = combine_results_by_view({"HistoBase100": dfh}).get("HistoBase100", pd.DataFrame()) if dfh else pd.DataFrame()
            process_performance_net(combined_h, top_params.get("selected_name"))
        with tabs[1]:
            process_risk_analysis(results_by_view)
        with tabs[2]:
            raw_preview_tab(results_by_view)

if __name__ == "__main__":
    main()

