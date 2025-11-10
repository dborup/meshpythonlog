# streamlit_dashboard_nodes_merged.py
# Meshtastic JSONL dashboard (merged across files) filtered to your 4 nodes
# + Alarms page with Telegram / Webhook notifications

from pathlib import Path
import json
from typing import List, Dict, Any, Optional
import pandas as pd
import plotly.express as px
import streamlit as st
import numpy as np
import time
import requests
from datetime import datetime, timedelta

# ---------------- Configuration ----------------
DEFAULT_DIR = "mesh_debug_logs"
JSONL_GLOB = "meshtastic_dump_*.jsonl"
TARGET_NODES = ["!335d6d57", "!dcb05c16", "!11ac3107", "!0bfc8344"]
NODE_NAMES = {
    "!335d6d57": "Drastrup",
    "!dcb05c16": "Øster-Alling",
    "!11ac3107": "Skejby",
    "!0bfc8344": "Skødstrup",
}

ALARM_STORE = Path("alarm_rules.json")
NOTIFY_STORE = Path("alarm_notify.json")

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

# Page switcher
st.sidebar.markdown("---")
page = st.sidebar.radio("Page", ["Dashboard", "Alarms"], index=0, horizontal=True)

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
# Apply friendly names
df["nodeName"] = df["fromId"].map(NODE_NAMES).fillna(df["fromId"]) 
# Telemetry-only subset
df_telem = df[df["portnum"] == "TELEMETRY_APP"].copy()

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


def downsample(df_in: pd.DataFrame, y_cols: list[str], freq: str) -> pd.DataFrame:
    # Keep only needed columns, sort by time
    cols = ["timestamp", "fromId"] + [c for c in y_cols if c in df_in.columns]
    d = df_in[cols].dropna(subset=["timestamp"]).copy()
    if d.empty:
        return d

    # ensure numeric so resample doesn't drop
    for c in y_cols:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")

    d = d.sort_values("timestamp")

    # resample per node
    chunks = []
    for nid, g in d.groupby("fromId"):
        if g.empty:
            continue
        g = g.set_index("timestamp")
        # resample only selected columns, by mean
        agg = g[y_cols].resample(freq).mean()
        agg["fromId"] = nid
        agg = agg.reset_index()
        chunks.append(agg)

    if not chunks:
        return pd.DataFrame(columns=["timestamp", "fromId"] + y_cols)

    out = pd.concat(chunks, ignore_index=True)

    # drop rows where *all* requested y-cols are NaN (keeps lines from disappearing)
    if y_cols:
        mask = out[y_cols].notna().any(axis=1)
        out = out[mask]

    return out


window_seconds = (end - start).total_seconds() if pd.notna(start) and pd.notna(end) else 0
res = choose_freq(window_seconds) if do_downsample else None

# ------------- Notification helpers & state -------------

def load_rules() -> list:
    if ALARM_STORE.exists():
        try:
            return json.loads(ALARM_STORE.read_text("utf-8"))
        except Exception:
            return []
    return []


def save_rules(rules: list):
    ALARM_STORE.write_text(json.dumps(rules, indent=2, ensure_ascii=False), encoding="utf-8")


def load_notify_cfg() -> dict:
    if NOTIFY_STORE.exists():
        try:
            return json.loads(NOTIFY_STORE.read_text("utf-8"))
        except Exception:
            return {"telegram": {}, "webhooks": []}
    return {"telegram": {}, "webhooks": []}


def save_notify_cfg(cfg: dict):
    NOTIFY_STORE.write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")


def send_telegram(bot_token: str, chat_id: str, text: str) -> tuple[bool, str]:
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        r = requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=10)
        if r.ok:
            return True, "sent"
        return False, f"HTTP {r.status_code}: {r.text[:200]}"
    except Exception as e:
        return False, str(e)


def send_webhook(url: str, payload: dict) -> tuple[bool, str]:
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.ok:
            return True, "sent"
        return False, f"HTTP {r.status_code}: {r.text[:200]}"
    except Exception as e:
        return False, str(e)


def latest_nonnull(g: pd.DataFrame, col: str):
    g2 = g.dropna(subset=[col])
    if g2.empty:
        return None, None
    last = g2.sort_values("timestamp").iloc[-1]
    return last[col], last["timestamp"]


# ------------- Evaluate alarms -------------

def evaluate_and_notify(df: pd.DataFrame, rules: list, notify_cfg: dict) -> list:
    """Check rules against latest values per node/metric. Return list of trigger logs."""
    logs = []
    now_dt = datetime.utcnow()

    # Build quick index for latest values per (node, metric)
    metrics = set()
    for r in rules:
        metrics.add(r.get("metric"))
    latest_map: dict[tuple[str, str], dict] = {}
    for metric in metrics:
        if metric not in df.columns:
            continue
        for nid, g in df.groupby("fromId"):
            val, ts = latest_nonnull(g, metric)
            if val is None or pd.isna(val):
                continue
            latest_map[(nid, metric)] = {"value": float(val), "ts": pd.to_datetime(ts)}

    # Iterate rules
    updated = False
    for r in rules:
        if not r.get("enabled", True):
            continue
        metric = r.get("metric")
        comparator = r.get("comparator", ">=")
        threshold = float(r.get("threshold", 0))
        cool_min = int(r.get("cooldown_min", 30))
        nodes = r.get("nodes") or TARGET_NODES

        last_fire_iso = r.get("last_fire")
        last_fire = datetime.fromisoformat(last_fire_iso) if last_fire_iso else None
        on_cooldown = last_fire and (now_dt - last_fire) < timedelta(minutes=cool_min)

        for nid in nodes:
            key = (nid, metric)
            if key not in latest_map:
                continue
            val = latest_map[key]["value"]
            ts = latest_map[key]["ts"]

            ok = False
            if comparator == ">":
                ok = val > threshold
            elif comparator == ">=":
                ok = val >= threshold
            elif comparator == "<":
                ok = val < threshold
            elif comparator == "<=":
                ok = val <= threshold
            elif comparator == "==":
                ok = val == threshold
            elif comparator == "!=":
                ok = val != threshold

            if ok and not on_cooldown:
                node_name = NODE_NAMES.get(nid, nid)
                msg = r.get("message") or f"Alarm: {node_name} {metric} {comparator} {threshold}. Value={val:.3f} @ {ts}"
                # Send notifications
                sent_any = False
                errors = []
                # Telegram
                tg = notify_cfg.get("telegram") or {}
                if tg.get("enabled") and tg.get("bot_token") and tg.get("chat_id"):
                    ok_s, info = send_telegram(tg["bot_token"], tg["chat_id"], msg)
                    sent_any = sent_any or ok_s
                    if not ok_s: errors.append(f"telegram: {info}")
                # Webhooks
                for wh in notify_cfg.get("webhooks", []):
                    if not wh.get("enabled"): continue
                    payload = {
                        "type": "meshtastic_alarm",
                        "node": nid,
                        "nodeName": node_name,
                        "metric": metric,
                        "comparator": comparator,
                        "threshold": threshold,
                        "value": val,
                        "timestamp": str(ts),
                        "message": msg,
                    }
                    ok_w, info = send_webhook(wh.get("url"), payload)
                    sent_any = sent_any or ok_w
                    if not ok_w: errors.append(f"webhook: {info}")

                r["last_fire"] = now_dt.isoformat()
                updated = True

                logs.append({
                    "rule": r.get("name") or f"{metric} {comparator} {threshold}",
                    "node": nid,
                    "nodeName": node_name,
                    "value": val,
                    "ts": str(ts),
                    "sent": sent_any,
                    "errors": "; ".join(errors) if errors else "",
                    "message": msg,
                })

    if updated:
        save_rules(rules)

    return logs


# ------------------------ UI: Dashboard ------------------------
if page == "Dashboard":
    # ---------------- KPIs (latest non-null per metric) ----------------
    st.subheader("Latest KPIs per target node")

    cards = st.columns(min(4, df["fromId"].nunique()))

    for i, node in enumerate(sorted(df["fromId"].unique())):
        g = df[df["fromId"] == node]
        name = NODE_NAMES.get(node, node)
        batt, batt_ts = latest_nonnull(g, "batteryLevel")
        devv, devv_ts = latest_nonnull(g, "voltage")
        rssi, rssi_ts = latest_nonnull(g, "rxRssi")
        snr,  snr_ts  = latest_nonnull(g, "rxSnr")

        # --- Uptime / Last seen (prepare text, but don't render yet) ---
        last_ts = g["timestamp"].dropna().max()
        if pd.notna(last_ts):
            delta = pd.Timestamp.utcnow().tz_localize(None) - last_ts
            minutes = delta.total_seconds() / 60
            if minutes < 600:
                uptime_txt = "🟢 Active"
            elif minutes < 1000:
                uptime_txt = f"🟡 Idle ({int(minutes // 60)} h ago)"
            elif minutes < 1440:
                uptime_txt = f"🟠 Offline ({int(minutes // 60)} h ago)"
            else:
                uptime_txt = f"🔴 Offline ({int(minutes // 1440)} d ago)"
        else:
            uptime_txt = "❓ No data"

        # (moved into the card below)

        hops, _ = latest_nonnull(g, "hopsTraversed")
        if hops is None or pd.isna(hops):
            hs, _ = latest_nonnull(g, "hopStart")
            hl, _ = latest_nonnull(g, "hopLimit")
            hops = (hs - hl) if (hs is not None and hl is not None) else None

        with cards[i % len(cards)]:
            st.markdown(f"**{name}** {uptime_txt.split()[0]}")

            # Uptime line now LIVES INSIDE the card
            st.caption(" ".join(uptime_txt.split()[1:]) or uptime_txt)

            batt_txt = "-" if batt is None else f"{batt:.0f}%"
            batt_age = "" if batt_ts is None else f" {pd.to_datetime(batt_ts)}"
            st.metric("Battery %", batt_txt)
            if batt_age:
                st.caption(batt_age)

            devv_txt = "-" if devv is None else f"{devv:.3f} V"
            st.metric("Device V", devv_txt)

            hops_txt = "-" if hops is None or pd.isna(hops) else f"{int(hops)}"
            st.metric("Hops away", hops_txt)

            au, _ = latest_nonnull(g, "airUtilTx")
            cu, _ = latest_nonnull(g, "channelUtilization")
            au_txt = "-" if au is None or pd.isna(au) else f"{au:.2f}%"
            cu_txt = "-" if cu is None or pd.isna(cu) else f"{cu:.2f}%"
            st.caption(f"Util: Air {au_txt} · Channel {cu_txt}")

            rssi_txt = "-" if rssi is None else f"{rssi:.0f} dBm"
            snr_txt  = "-" if snr  is None else f"{snr:.1f} dB"
            st.caption(f"RSSI {rssi_txt}, SNR {snr_txt}")

    # ---------------- Global Expand/Collapse toggle ----------------
    st.sidebar.markdown("---")
    expand_all = st.sidebar.checkbox("Expand all charts", value=False)

    # ---------------- Auto-refresh ----------------
    st.sidebar.markdown("---")
    autorefresh = st.sidebar.checkbox("Auto-refresh dashboard", value=True)
    refresh_interval = st.sidebar.number_input("Refresh interval (seconds)", 5, 600, 300)
    if autorefresh:
        st.experimental_rerun() if time.time() % refresh_interval < 1 else None

    # ---------------- Charts ----------------
    def line_chart(df_in: pd.DataFrame, y: str, title: str, expanded=False):
        expanded = expanded or expand_all
        if y not in df_in.columns or df_in[y].isna().all():
            return
        data = downsample(df_in, [y], res) if do_downsample else df_in[["timestamp", "fromId", y]].dropna()
        if data.empty or y not in data.columns:
            return
        data["nodeName"] = data["fromId"].map(NODE_NAMES).fillna(data["fromId"])    
        fig = px.line(
            data,
            x="timestamp",
            y=y,
            color="nodeName",
            title=f"{title}{'' if not do_downsample else f' (res={res})'}",
            labels={"nodeName": "Node"}
        )
        with st.expander(title, expanded=expanded):
            st.plotly_chart(fig, use_container_width=True)

    st.header("Power & Battery")
    line_chart(df_telem, "batteryLevel", "Battery %", expanded=True)
    line_chart(df_telem, "voltage", "Device Voltage (V)")

    for ch in (1, 2, 3):
        line_chart(df_telem, f"ina{ch}Voltage", f"INA{ch} Voltage (V)")
        line_chart(df_telem, f"ina{ch}Current", f"INA{ch} Current (A)")
        line_chart(df_telem, f"ina{ch}Power",   f"INA{ch} Power (W)")

    st.header("Radio")
    line_chart(df_telem, "rxRssi", "RSSI (dBm)")
    line_chart(df_telem, "rxSnr", "SNR (dB)")
    line_chart(df_telem, "channelUtilization", "Channel Utilization")
    line_chart(df_telem, "airUtilTx", "Air Util Tx")

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
    preview_cols.insert(1, "nodeName")
    st.dataframe(df.sort_values("timestamp")[preview_cols].tail(200), use_container_width=True)

# ------------------------ UI: Alarms ------------------------
else:
    st.header("🔔 Alarms")

    # Load current
    rules = load_rules()
    notify_cfg = load_notify_cfg()

    with st.expander("Notification channels", expanded=True):
        tg_enabled = st.checkbox("Enable Telegram", value=bool(notify_cfg.get("telegram", {}).get("enabled", False)))
        tg_bot = st.text_input("Telegram Bot Token", type="password", value=notify_cfg.get("telegram", {}).get("bot_token", ""))
        tg_chat = st.text_input("Telegram Chat ID", value=notify_cfg.get("telegram", {}).get("chat_id", ""))
        if st.button("Save Telegram settings"):
            notify_cfg.setdefault("telegram", {})
            notify_cfg["telegram"].update({"enabled": tg_enabled, "bot_token": tg_bot.strip(), "chat_id": tg_chat.strip()})
            save_notify_cfg(notify_cfg)
            st.success("Saved Telegram settings")

        st.markdown("---")
        st.subheader("Webhooks (e.g., ntfy.sh, IFTTT, Home Assistant)")
        existing = notify_cfg.get("webhooks", [])
        for idx, wh in enumerate(existing):
            cols = st.columns([6,2,2])
            with cols[0]:
                url = st.text_input(f"Webhook URL #{idx+1}", value=wh.get("url", ""), key=f"wh_url_{idx}")
            with cols[1]:
                enabled = st.checkbox("Enabled", value=wh.get("enabled", True), key=f"wh_en_{idx}")
            with cols[2]:
                if st.button("Delete", key=f"wh_del_{idx}"):
                    existing.pop(idx)
                    notify_cfg["webhooks"] = existing
                    save_notify_cfg(notify_cfg)
                    st.experimental_rerun()
            wh["url"] = url
            wh["enabled"] = enabled
        if st.button("Save webhooks"):
            notify_cfg["webhooks"] = existing
            save_notify_cfg(notify_cfg)
            st.success("Saved webhooks")

    st.markdown("---")

    # Create new rule
    st.subheader("Create / Update Rule")
    with st.form("new_rule"):
        name = st.text_input("Name", placeholder="Battery low Skejby")
        nodes = st.multiselect("Nodes", options=sorted(df["fromId"].unique()), format_func=lambda x: f"{NODE_NAMES.get(x, x)} ({x})")
        metric = st.selectbox("Metric", options=[c for c in [
            "batteryLevel","voltage","rxRssi","rxSnr","airUtilTx","channelUtilization",
            "ina1Voltage","ina1Current","ina1Power",
            "ina2Voltage","ina2Current","ina2Power",
            "ina3Voltage","ina3Current","ina3Power",
            "hopStart","hopLimit","hopsTraversed"
        ] if c in df.columns])
        comparator = st.selectbox("When value is…", options=[">=", ">", "<=", "<", "==", "!="], index=0)
        threshold = st.number_input("Threshold", value=20.0)
        cooldown = st.number_input("Cooldown (minutes)", min_value=1, max_value=1440, value=60)
        message = st.text_input("Message (optional)", placeholder="⚠️ {nodeName} {metric}={value:.1f} at {ts}")
        enabled = st.checkbox("Enabled", value=True)
        submitted = st.form_submit_button("Save rule")

    if submitted:
        rule = {
            "name": name or f"{metric} {comparator} {threshold}",
            "nodes": nodes or TARGET_NODES,
            "metric": metric,
            "comparator": comparator,
            "threshold": threshold,
            "cooldown_min": int(cooldown),
            "message": message,
            "enabled": enabled,
        }
        # Upsert by name
        replaced = False
        for i, r in enumerate(rules):
            if r.get("name") == rule["name"]:
                rules[i] = {**r, **rule}
                replaced = True
                break
        if not replaced:
            rules.append(rule)
        save_rules(rules)
        st.success("Rule saved")

    # Rules table & quick actions
    st.subheader("Existing Rules")
    if not rules:
        st.info("No rules yet. Create one above.")
    else:
        for i, r in enumerate(rules):
            with st.expander(f"{r.get('name')}  —  {r.get('metric')} {r.get('comparator')} {r.get('threshold')}", expanded=False):
                cols = st.columns([2,2,2,2,2,1,1])
                cols[0].markdown("**Nodes**\n" + ", ".join([NODE_NAMES.get(n, n) for n in (r.get("nodes") or [])]))
                cols[1].markdown(f"**Metric**\n{r.get('metric')}")
                cols[2].markdown(f"**Comparator**\n{r.get('comparator')}")
                cols[3].markdown(f"**Threshold**\n{r.get('threshold')}")
                cols[4].markdown(f"**Cooldown**\n{r.get('cooldown_min')} min")
                cols[5].markdown("**Enabled**\n" + ("✅" if r.get("enabled", True) else "❌"))
                last_f = r.get("last_fire")
                cols[6].markdown("**Last trigger**\n" + (last_f or "–"))
                c2 = st.columns([1,1,1])
                if c2[0].button("Toggle", key=f"toggle_{i}"):
                    r["enabled"] = not r.get("enabled", True)
                    save_rules(rules); st.experimental_rerun()
                if c2[1].button("Test now", key=f"test_{i}"):
                    # Force a test notification with dummy value
                    node = (r.get("nodes") or TARGET_NODES)[0]
                    nodeName = NODE_NAMES.get(node, node)
                    msg = r.get("message") or f"Alarm test: {r.get('name')}"
                    ok_any = False; errors=[]
                    tg = notify_cfg.get("telegram") or {}
                    if tg.get("enabled") and tg.get("bot_token") and tg.get("chat_id"):
                        ok, info = send_telegram(tg["bot_token"], tg["chat_id"], msg + f" (test from {nodeName})")
                        ok_any = ok_any or ok
                        if not ok: errors.append(f"telegram: {info}")
                    for wh in notify_cfg.get("webhooks", []):
                        if not wh.get("enabled"): continue
                        ok, info = send_webhook(wh.get("url"), {"message": msg, "test": True, "rule": r.get("name")})
                        ok_any = ok_any or ok
                        if not ok: errors.append(f"webhook: {info}")
                    if ok_any:
                        st.success("Test notification sent")
                    else:
                        st.error("No notification sent: " + ("; ".join(errors) or "no channels configured"))
                if c2[2].button("Delete", key=f"del_{i}"):
                    rules.pop(i); save_rules(rules); st.experimental_rerun()

    st.markdown("---")
    st.subheader("Live evaluation & recent triggers")
    logs = evaluate_and_notify(df, rules, notify_cfg)
    if logs:
        st.success(f"{len(logs)} trigger(s) fired this refresh")
        st.dataframe(pd.DataFrame(logs), use_container_width=True)
    else:
        st.caption("No triggers this refresh.")
