# streamlit_dashboard_nodes_merged.py
# Meshtastic JSONL dashboard (merged across files) filtered to your 4 nodes.

from pathlib import Path
import json
from typing import List, Dict, Any, Optional
import pandas as pd
import plotly.express as px
import streamlit as st
import numpy as np

# ---------------- Configuration ----------------
DEFAULT_DIR = "mesh_debug_logs"
JSONL_GLOB = "meshtastic_dump_*.jsonl"
TARGET_NODES = ["!335d6d57", "!dcb05c16", "!11ac3107", "!0bfc8344"]

st.set_page_config(page_title="Meshtastic Node Dashboard (Merged)", layout="wide")
st.title("📡 Meshtastic Node Dashboard — merged JSONL, filtered to 4 nodes")

# ---------------- Sidebar ----------------
st.sidebar.header("Source")
log_dir = Path(st.sidebar.text_input("Log directory", value=str(DEFAULT_DIR)))

if not log_dir.exists():
    st.error(f"Log directory not found: {log_dir.resolve()}")
    st.stop()

# All JSONL files (sorted by modification time ascending)
files = sorted(log_dir.glob(JSONL_GLOB), key=lambda p: p.stat().st_mtime)
if not files:
    st.error(f"No JSONL files found in {log_dir.resolve()} (pattern: {JSONL_GLOB})")
    st.stop()

st.sidebar.write(f"Found {len(files)} JSONL files.")
max_rows_total = st.sidebar.slider("Read last N lines (across all files)", 1_000, 500_000, 50_000, 1_000)
st.sidebar.caption("Data is merged from all files; newest lines win if duplicates exist.")

# Time window presets
now = pd.Timestamp.utcnow().tz_localize(None)
preset = st.sidebar.selectbox("Time range", ["1h", "6h", "24h", "72h", "7d", "All"], index=2)
if preset == "All":
    start = pd.Timestamp.min.tz_localize(None)
    end = pd.Timestamp.max.tz_localize(None)
else:
    start, end = now - pd.to_timedelta(preset), now

# Downsample for performance
do_downsample = st.sidebar.checkbox("Downsample (time-bin mean)", value=True)

# Manual refresh
if st.sidebar.button("Refresh"):
    st.cache_data.clear()
    st.experimental_rerun()

# ---------------- JSON parsing helpers ----------------
def _safe_parse(line: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(line)
    except Exception:
        return None

def _extract_row(j: Dict[str, Any]) -> Dict[str, Any]:
    pkt = j.get("packet", {}) or {}
    dec = pkt.get("decoded", {}) or {}
    telem = dec.get("telemetry") or {}

    row: Dict[str, Any] = {
        "timestamp": j.get("timestamp") or j.get("ts") or j.get("time"),
        "fromId": pkt.get("fromId") or pkt.get("from"),
        "toId": pkt.get("toId") or pkt.get("to"),
        "portnum": dec.get("portnum"),
        "rxRssi": pkt.get("rxRssi"),
        "rxSnr": pkt.get("rxSnr"),
        "hopStart": pkt.get("hopStart"),
        "hopLimit": pkt.get("hopLimit"),
        "relayNode": pkt.get("relayNode"),
        # device
        "batteryLevel": None,
        "voltage": None,
        "airUtilTx": None,
        "channelUtilization": None,
        # INA / power
        "ina1Voltage": None, "ina1Current": None, "ina1Power": None,
        "ina2Voltage": None, "ina2Current": None, "ina2Power": None,
        "ina3Voltage": None, "ina3Current": None, "ina3Power": None,
    }

    dev = telem.get("deviceMetrics") or (dec.get("payload") or {}).get("deviceMetrics") or {}
    row["batteryLevel"] = dev.get("batteryLevel")
    row["voltage"] = dev.get("voltage")
    row["airUtilTx"] = dev.get("airUtilTx")
    row["channelUtilization"] = dev.get("channelUtilization")
    # Fallback: some firmwares report these in localStats instead of deviceMetrics
    ls = telem.get("localStats") or (dec.get("payload") or {}).get("localStats") or {}
    if row["channelUtilization"] is None:
        row["channelUtilization"] = ls.get("channelUtilization")
    if row["airUtilTx"] is None:
        row["airUtilTx"] = ls.get("airUtilTx")    

    # powerMetrics: map many variants -> inaX*
    pm = telem.get("powerMetrics") or (dec.get("payload") or {}).get("powerMetrics") or {}
    if isinstance(pm, dict):
        def g(*keys):
            for k in keys:
                if k in pm:
                    return pm[k]
            return None
        # ch1/ch2/ch3 Voltage/Current/Power variants
        row["ina1Voltage"] = g("ina3221Ch1Voltage", "vBus1", "ch1Voltage", "voltage1", "busVoltage1")
        row["ina1Current"] = g("ina3221Ch1Current", "iBus1", "ch1Current", "current1")
        row["ina1Power"]   = g("ina3221Ch1Power",   "pBus1", "ch1Power",   "power1")

        row["ina2Voltage"] = g("ina3221Ch2Voltage", "vBus2", "ch2Voltage", "voltage2", "busVoltage2")
        row["ina2Current"] = g("ina3221Ch2Current", "iBus2", "ch2Current", "current2")
        row["ina2Power"]   = g("ina3221Ch2Power",   "pBus2", "ch2Power",   "power2")

        row["ina3Voltage"] = g("ina3221Ch3Voltage", "vBus3", "ch3Voltage", "voltage3", "busVoltage3")
        row["ina3Current"] = g("ina3221Ch3Current", "iBus3", "ch3Current", "current3")
        row["ina3Power"]   = g("ina3221Ch3Power",   "pBus3", "ch3Power",   "power3")

    return row

@st.cache_data(show_spinner=False)
def load_merge_tail(paths: List[Path], total_tail: int) -> pd.DataFrame:
    """
    Load all files, keep only the last `total_tail` lines across the merged stream.
    Simple approach: read all lines (with filename tag), then slice the tail.
    """
    all_lines: List[Dict[str, Any]] = []
    for p in paths:
        try:
            lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()
        except Exception:
            continue
        for ln in lines:
            j = _safe_parse(ln)
            if j:
                all_lines.append({"_f": p.name, "_j": j})
    if not all_lines:
        return pd.DataFrame()

    # Keep the newest N lines across all files
    tail = all_lines[-total_tail:]
    rows = [_extract_row(it["_j"]) for it in tail]
    df = pd.DataFrame(rows)

    # Normalize types
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        # Make tz-naive for simple comparisons
        try:
            df["timestamp"] = df["timestamp"].dt.tz_convert(None)
        except Exception:
            try:
                df["timestamp"] = df["timestamp"].dt.tz_localize(None)
            except Exception:
                pass

    for col in ["fromId", "toId", "portnum"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) else x)

    return df

with st.status(f"Merging {len(files)} files …", expanded=False):
    df = load_merge_tail(files, max_rows_total)
        # Ensure numeric dtypes so downsampling keeps these columns
    for col in [
        "batteryLevel", "voltage",
        "channelUtilization", "airUtilTx",
        "ina1Voltage", "ina1Current", "ina1Power",
        "ina2Voltage", "ina2Current", "ina2Power",
        "ina3Voltage", "ina3Current", "ina3Power",
        "rxRssi", "rxSnr",
        "hopStart", "hopLimit", "hopsTraversed",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Derived hop metrics

if "hopStart" in df.columns and "hopLimit" in df.columns:
    df["hopsTraversed"] = np.where(
        df[["hopStart", "hopLimit"]].notna().all(axis=1),
        df["hopStart"].astype("float") - df["hopLimit"].astype("float"),
        np.nan,
    )

# Optional: flag if relayed (some packets expose relayNode)
if "relayNode" in df.columns:
    df["relayed"] = df["relayNode"].notna().astype("int")


if df.empty:
    st.info("No parseable rows found.")
    st.stop()

# Filter to target nodes
df = df[df["fromId"].isin(TARGET_NODES)]
if df.empty:
    st.warning("No data for the target nodes in the selected files.")
    st.stop()

# Time filter
df = df[(df["timestamp"] >= start) & (df["timestamp"] <= end)]
if df.empty:
    st.info("No rows in the chosen time window for the target nodes.")
    st.stop()

# ---------------- Downsampling ----------------
def choose_freq(seconds: float) -> str:
    if seconds <= 3600: return "5s"
    if seconds <= 6*3600: return "30s"
    if seconds <= 24*3600: return "2min"
    if seconds <= 72*3600: return "5min"
    if seconds <= 7*24*3600: return "15min"
    return "1H"

def downsample(df_in: pd.DataFrame, ycols: List[str], freq: str) -> pd.DataFrame:
    needed = ["timestamp", "fromId"] + [c for c in ycols if c in df_in.columns]
    work = df_in[needed].dropna(subset=["timestamp"]).copy()
    if work.empty:
        return work
    work = work.set_index("timestamp")
    agg = work.groupby([pd.Grouper(freq=freq), "fromId"]).mean(numeric_only=True)
    return agg.reset_index()

window_seconds = (end - start).total_seconds() if pd.notna(start) and pd.notna(end) else 0
res = choose_freq(window_seconds) if do_downsample else None

# ---------------- KPIs ----------------
# ---------------- KPIs (latest non-null per metric) ----------------
st.subheader("Latest KPIs per target node")

def latest_nonnull(g, col):
    g2 = g.dropna(subset=[col])
    if g2.empty:
        return None, None
    last = g2.sort_values("timestamp").iloc[-1]
    return last[col], last["timestamp"]

cards = st.columns(min(4, df["fromId"].nunique()))

for i, node in enumerate(sorted(df["fromId"].unique())):
    g = df[df["fromId"] == node]
    batt, batt_ts = latest_nonnull(g, "batteryLevel")
    devv, devv_ts = latest_nonnull(g, "voltage")
    rssi, rssi_ts = latest_nonnull(g, "rxRssi")
    snr,  snr_ts  = latest_nonnull(g, "rxSnr")
    iv, iv_ts = latest_nonnull(g, "ina1Voltage")
    ic, ic_ts = latest_nonnull(g, "ina1Current")
    ip, ip_ts = latest_nonnull(g, "ina1Power")

    with cards[i % len(cards)]:
        st.markdown(f"**{node}**")

        # Battery
        batt_txt = "-" if batt is None else f"{batt:.0f}%"
        batt_age = "" if batt_ts is None else f" · seen {pd.to_datetime(batt_ts)}"
        st.metric("Battery %", batt_txt)
        if batt_age:
            st.caption(batt_age)

        # Device V
        devv_txt = "-" if devv is None else f"{devv:.3f} V"
        devv_age = "" if devv_ts is None else f" · seen {pd.to_datetime(devv_ts)}"
        st.metric("Device V", devv_txt)
        if devv_age:
            st.caption(devv_age)

        # INA1 summary line
        iv_txt = "-" if iv is None else f"{iv:.3f}"
        ic_txt = "-" if ic is None else f"{ic:.3f}"
        st.caption(f"INA1 V/A: {iv_txt} / {ic_txt}")

        # INA2 summary line
        iv2, iv2_ts = latest_nonnull(g, "ina2Voltage")
        ic2, ic2_ts = latest_nonnull(g, "ina2Current")
        iv2_txt = "-" if iv2 is None else f"{iv2:.3f}"
        ic2_txt = "-" if ic2 is None else f"{ic2:.3f}"
        st.caption(f"INA2 V/A: {iv2_txt} / {ic2_txt}")

        # Radio
        rssi_txt = "-" if rssi is None else f"{rssi:.0f} dBm"
        snr_txt  = "-" if snr  is None else f"{snr:.1f} dB"
        st.caption(f"RSSI {rssi_txt}, SNR {snr_txt}")


# ---------------- Charts ----------------
def line_chart(df_in: pd.DataFrame, y: str, title: str, expanded=False):
    if y not in df_in.columns or df_in[y].isna().all():
        return
    data = downsample(df_in, [y], res) if do_downsample else df_in[["timestamp", "fromId", y]].dropna()
    if data.empty or y not in data.columns:
        return
    fig = px.line(data, x="timestamp", y=y, color="fromId",
                  title=f"{title}{'' if not do_downsample else f' (res={res})'}",
                  labels={"fromId": "Node"})
    with st.expander(title, expanded=expanded):
        st.plotly_chart(fig, use_container_width=True)

st.header("Power & Battery")
line_chart(df, "batteryLevel", "Battery %", expanded=True)
line_chart(df, "voltage", "Device Voltage (V)")

for ch in (1, 2, 3):
    line_chart(df, f"ina{ch}Voltage", f"INA{ch} Voltage (V)")
    line_chart(df, f"ina{ch}Current", f"INA{ch} Current (A)")
    line_chart(df, f"ina{ch}Power",   f"INA{ch} Power (W)")

st.header("Radio")
line_chart(df, "rxRssi", "RSSI (dBm)")
line_chart(df, "rxSnr", "SNR (dB)")
line_chart(df, "channelUtilization", "Channel Utilization")
line_chart(df, "airUtilTx", "Air Util Tx")

st.header("Hops")
line_chart(df, "hopStart", "Hop Start (initial TTL)")
line_chart(df, "hopLimit", "Hop Limit (remaining TTL)")
if "hopsTraversed" in df.columns:
    line_chart(df, "hopsTraversed", "Hops Traversed (hopStart - hopLimit)")
if "relayed" in df.columns and df["relayed"].notna().any():
    line_chart(df, "relayed", "Relayed (0/1)")


st.markdown("---")
st.subheader("Preview (last 200 rows for target nodes)")

preview_cols = [c for c in [
    "timestamp","fromId","portnum",
    "hopStart","hopLimit","hopsTraversed","relayed",
    "rxRssi","rxSnr",
    "batteryLevel","voltage",
    "ina1Voltage","ina1Current","ina2Voltage","ina2Current","ina3Voltage","ina3Current",
] if c in df.columns]
st.dataframe(df.sort_values("timestamp")[preview_cols].tail(200), use_container_width=True)

