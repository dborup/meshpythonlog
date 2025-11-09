#!/usr/bin/env python3
"""
Meshtastic TCP full debug listener + JSONL (bytes-safe) + CSV (flattened)
- Handles packets as dict/str/bytes/proto
- Parses decoded.telemetry: deviceMetrics, environmentMetrics, powerMetrics, localStats
- Parses decoded.user from NODEINFO_APP
- Parses WAYPOINT_APP fully (incl. expire + raw/b64) into JSONL and CSV
- Includes INA3221 parsing from powerMetrics / fallbacks
"""

import argparse, sys, time, json, base64, csv
from datetime import datetime
from pathlib import Path

import meshtastic
import meshtastic.tcp_interface
from pubsub import pub

# ---------------- JSON helpers (bytes-safe) ----------------

def jsonable(obj):
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, (bytes, bytearray)):
        return {"__bytes__": True, "b64": base64.b64encode(bytes(obj)).decode("ascii")}
    if isinstance(obj, dict):
        return {str(k): jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [jsonable(v) for v in obj]
    if hasattr(obj, "__dict__"):
        return jsonable(dict(obj.__dict__))
    try:
        return json.loads(json.dumps(obj, default=str))
    except Exception:
        return str(obj)

def ensure_dict(p):
    """Best-effort normalization to a JSON-like dict, mirroring jsonable() for bytes."""
    if isinstance(p, dict):
        return p
    if isinstance(p, (bytes, bytearray)):
        # Normalize to the SAME shape you use in JSONL for consistency
        return {"__bytes__": True, "b64": base64.b64encode(bytes(p)).decode("ascii")}
    if isinstance(p, str):
        try:
            j = json.loads(p)
            return j if isinstance(j, dict) else {"_str": p}
        except Exception:
            return {"_str": p}
    if hasattr(p, "__dict__"):
        return dict(p.__dict__)
    return {"_repr": str(p)}

def _asdict(x):
    if isinstance(x, dict): return x
    if hasattr(x, "__dict__"): return dict(x.__dict__)
    if isinstance(x, str):
        try:
            j = json.loads(x)
            return j if isinstance(j, dict) else {"_str": x}
        except Exception:
            return {"_str": x}
    if isinstance(x, (bytes, bytearray)):
        # Keep consistent with jsonable/ensure_dict
        return {"__bytes__": True, "b64": base64.b64encode(bytes(x)).decode("ascii")}
    try:
        j = json.loads(json.dumps(x, default=str))
        return j if isinstance(j, dict) else {"_repr": str(x)}
    except Exception:
        return {"_repr": str(x)}

def _first_of_list_or_dict(x):
    if isinstance(x, list) and x:
        return _asdict(x[0])
    return _asdict(x or {})

# ---------------- Power / INA3221 helpers ----------------

def _get_ina_field(d, *candidates):
    for k in candidates:
        if isinstance(d, dict) and k in d:
            return d[k]
    return None

def parse_ina3221_from_pm(pm: dict) -> dict:
    out = {
        "ina1Voltage": None, "ina1Current": None, "ina1Power": None, "ina1ShuntVoltage": None,
        "ina2Voltage": None, "ina2Current": None, "ina2Power": None, "ina2ShuntVoltage": None,
        "ina3Voltage": None, "ina3Current": None, "ina3Power": None, "ina3ShuntVoltage": None,
    }
    if not isinstance(pm, dict):
        return out

    ina = pm.get("ina3221")

    # dict of channels
    if isinstance(ina, dict):
        ch1 = ina.get("ch1") or ina.get("channel1") or ina.get("1")
        ch2 = ina.get("ch2") or ina.get("channel2") or ina.get("2")
        ch3 = ina.get("ch3") or ina.get("channel3") or ina.get("3")
        for idx, ch in ((1, ch1), (2, ch2), (3, ch3)):
            if isinstance(ch, dict):
                out[f"ina{idx}Voltage"]      = _get_ina_field(ch, "voltage", "busVoltage", "vBus", "v")
                out[f"ina{idx}Current"]      = _get_ina_field(ch, "current", "iBus", "i")
                out[f"ina{idx}Power"]        = _get_ina_field(ch, "power", "pBus", "p")
                out[f"ina{idx}ShuntVoltage"] = _get_ina_field(ch, "shuntVoltage", "vShunt", "shunt")

    # list of channels
    if isinstance(ina, list):
        for ch in ina:
            if not isinstance(ch, dict):
                continue
            idx = ch.get("channel") or ch.get("ch") or ch.get("index")
            if idx in (1, 2, 3):
                out[f"ina{idx}Voltage"]      = out[f"ina{idx}Voltage"]      or _get_ina_field(ch, "voltage", "busVoltage", "vBus", "v")
                out[f"ina{idx}Current"]      = out[f"ina{idx}Current"]      or _get_ina_field(ch, "current", "iBus", "i")
                out[f"ina{idx}Power"]        = out[f"ina{idx}Power"]        or _get_ina_field(ch, "power", "pBus", "p")
                out[f"ina{idx}ShuntVoltage"] = out[f"ina{idx}ShuntVoltage"] or _get_ina_field(ch, "shuntVoltage", "vShunt", "shunt")

    # flattened styles
    for idx in (1, 2, 3):
        out[f"ina{idx}Voltage"]      = out[f"ina{idx}Voltage"]      or pm.get(f"ina3221Ch{idx}Voltage") or pm.get(f"vBus{idx}")
        out[f"ina{idx}Current"]      = out[f"ina{idx}Current"]      or pm.get(f"ina3221Ch{idx}Current") or pm.get(f"iBus{idx}")
        out[f"ina{idx}Power"]        = out[f"ina{idx}Power"]        or pm.get(f"ina3221Ch{idx}Power")   or pm.get(f"pBus{idx}")
        out[f"ina{idx}ShuntVoltage"] = out[f"ina{idx}ShuntVoltage"] or pm.get(f"shuntVoltage{idx}")     or pm.get(f"vShunt{idx}")

    # simple chNVoltage/chNCurrent forms
    for idx in (1, 2, 3):
        out[f"ina{idx}Voltage"] = out[f"ina{idx}Voltage"] or pm.get(f"ch{idx}Voltage")
        out[f"ina{idx}Current"] = out[f"ina{idx}Current"] or pm.get(f"ch{idx}Current")

    return out

def parse_ina3221(decoded: dict) -> dict:
    telem = _asdict(decoded.get("telemetry", {}))
    pm = _asdict(telem.get("powerMetrics", {}))
    if pm:
        return parse_ina3221_from_pm(pm)
    payload = _asdict(decoded.get("payload", {}))
    pm = _asdict(payload.get("powerMetrics", {}))
    if pm:
        return parse_ina3221_from_pm(pm)
    ina = payload.get("ina3221")
    if isinstance(ina, (dict, list)):
        return parse_ina3221_from_pm({"ina3221": ina})
    return parse_ina3221_from_pm({})

# ---------------- WAYPOINT helper ----------------

def _payload_b64(payload_dict: dict):
    """Return base64 string regardless of bytes-wrapper shape."""
    if not isinstance(payload_dict, dict):
        return None
    if "b64" in payload_dict:  # our normalized / JSONL shape
        return payload_dict.get("b64")
    if "_bytes_b64" in payload_dict:  # legacy shape (if any old logs exist)
        return payload_dict.get("_bytes_b64")
    return None

def _extract_waypoint_fields(pkt: dict) -> dict:
    """
    Pulls every useful bit from a WAYPOINT_APP packet and keeps
    both human-friendly fields and raw values for lossless storage.
    """
    out = {}

    dec = _asdict((pkt or {}).get("decoded", {}))
    wpt = _asdict(dec.get("waypoint") or {})

    # Convenience: float lat/lon if present (prefer float, else convert *I)
    lat = wpt.get("latitude")
    if lat is None and "latitudeI" in wpt:
        try:
            lat = wpt["latitudeI"] / 1e7
        except Exception:
            lat = None

    lon = wpt.get("longitude")
    if lon is None and "longitudeI" in wpt:
        try:
            lon = wpt["longitudeI"] / 1e7
        except Exception:
            lon = None

    payload_dict = _asdict(dec.get("payload"))
    payload_b64 = _payload_b64(payload_dict)

    out.update({
        # friendly fields
        "waypointId": wpt.get("id"),
        "waypointLat": lat,
        "waypointLon": lon,
        "waypointName": wpt.get("name"),
        "waypointDescription": wpt.get("description"),
        "waypointIcon": wpt.get("icon"),
        "waypointLockedTo": wpt.get("lockedTo"),
        # expire can be epoch/seconds or boolean; keep raw + bool
        "waypointExpire": bool(wpt.get("expire", 0)),
        "waypointExpireRaw": wpt.get("expire"),

        # raw integer coords and any vendor float fields if they exist
        "waypointLatitudeI": wpt.get("latitudeI"),
        "waypointLongitudeI": wpt.get("longitudeI"),
        "waypointLatitudeRaw": wpt.get("latitude"),
        "waypointLongitudeRaw": wpt.get("longitude"),

        # keep the raw decoded payload b64 for replay/debug
        "waypointPayloadB64": payload_b64,

        # keep meta we often need later
        "portnum": dec.get("portnum"),
        "fromId": pkt.get("fromId") or (pkt.get("from") if isinstance(pkt.get("from"), str) else None),
        "toId": pkt.get("toId"),
        "rxTime": pkt.get("rxTime") or _asdict(pkt.get("telemetry")).get("time"),
        "rxSnr": pkt.get("rxSnr"),
        "rxRssi": pkt.get("rxRssi"),
        "hopLimit": pkt.get("hopLimit"),
        "bitfield": dec.get("bitfield") or pkt.get("bitfield"),
    })

    return out

# ---------------- High-level flattener (JSONL convenience) ----------------

def flatten_packet(p):
    pkt = p.get("packet", {}) or {}
    dec = pkt.get("decoded", {}) or {}

    out = {
        "timestamp": p.get("timestamp"),
        "id": pkt.get("id"),
        "fromId": pkt.get("fromId"),
        "toId": pkt.get("toId"),
        "portnum": dec.get("portnum"),
        "priority": pkt.get("priority"),
        "rxTime": pkt.get("rxTime"),
        "rxSnr": pkt.get("rxSnr"),
        "rxRssi": pkt.get("rxRssi"),
        "hopLimit": pkt.get("hopLimit"),
        "hopStart": pkt.get("hopStart"),
        "relayNode": pkt.get("relayNode"),
        "channel": pkt.get("channel"),
        "encrypted": pkt.get("encrypted"),
    }

    telem = dec.get("telemetry") or {}
    pos   = dec.get("position") or {}
    user  = dec.get("user") or {}
    rout  = dec.get("routing") or {}

    # POSITION_APP
    if dec.get("portnum") == "POSITION_APP" or pos:
        lat = pos.get("latitude")
        if lat is None and "latitudeI" in pos:
            lat = pos["latitudeI"] / 1e7
        lon = pos.get("longitude")
        if lon is None and "longitudeI" in pos:
            lon = pos["longitudeI"] / 1e7
        out.update({
            "latitude": lat,
            "longitude": lon,
            "altitude": pos.get("altitude"),
            "positionTime": pos.get("time"),
            "locationSource": pos.get("locationSource"),
            "PDOP": pos.get("PDOP"),
            "satsInView": pos.get("satsInView"),
            "groundSpeed": pos.get("groundSpeed"),
            "groundTrack": pos.get("groundTrack"),
            "precisionBits": pos.get("precisionBits"),
            "positionTimestamp": pos.get("timestamp"),
        })

    # TELEMETRY (deviceMetrics)
    dm = (telem or {}).get("deviceMetrics") or {}
    if dm:
        out.update({
            "batteryLevel": dm.get("batteryLevel"),
            "pmVoltage": dm.get("voltage"),
            "channelUtilization": dm.get("channelUtilization"),
            "airUtilTx": dm.get("airUtilTx"),
            "uptimeSeconds": dm.get("uptimeSeconds"),
        })

    # TELEMETRY (environmentMetrics)
    em = (telem or {}).get("environmentMetrics") or {}
    if em:
        out.update({
            "temperature": em.get("temperature"),
            "relativeHumidity": em.get("relativeHumidity"),
            "barometricPressure": em.get("barometricPressure"),
            "gasResistance": em.get("gasResistance"),
            "envVoltage": em.get("voltage"),
            "envCurrent": em.get("current"),
            "iaq": em.get("iaq"),
        })

    # TELEMETRY (localStats)
    ls = (telem or {}).get("localStats") or {}
    if ls:
        out.update({
            "numPacketsTx": ls.get("numPacketsTx"),
            "numPacketsRx": ls.get("numPacketsRx"),
            "numPacketsRxBad": ls.get("numPacketsRxBad"),
            "numOnlineNodes": ls.get("numOnlineNodes"),
            "numTotalNodes": ls.get("numTotalNodes"),
            "numRxDupe": ls.get("numRxDupe"),
            "numTxRelay": ls.get("numTxRelay"),
            "numTxRelayCanceled": ls.get("numTxRelayCanceled"),
            "heapTotalBytes": ls.get("heapTotalBytes"),
            "heapFreeBytes": ls.get("heapFreeBytes"),
            "uptimeSeconds_local": ls.get("uptimeSeconds"),
            "channelUtilization_local": ls.get("channelUtilization"),
            "airUtilTx_local": ls.get("airUtilTx"),
        })

    # NODEINFO_APP
    if user:
        out.update({
            "userId": user.get("id"),
            "userLongName": user.get("longName"),
            "userShortName": user.get("shortName"),
            "macaddr": user.get("macaddr"),
            "hwModel": user.get("hwModel"),
            "publicKey": user.get("publicKey"),
            "isUnmessagable": user.get("isUnmessagable"),
        })

    # WAYPOINT_APP
    if dec.get("portnum") == "WAYPOINT_APP" or dec.get("waypoint"):
        out.update(_extract_waypoint_fields(pkt))

    # ROUTING_APP
    if dec.get("portnum") == "ROUTING_APP" or rout:
        out.update({
            "routingRequestId": dec.get("requestId"),
            "routingErrorReason": rout.get("errorReason"),
            "isAck": (pkt.get("priority") == "ACK"),
            "routingPayloadB64": _payload_b64(_asdict(dec.get("payload"))),
        })

    return out

# ---------------- CSV schema ----------------

CSV_COLUMNS = [
    # meta
    "timestamp", "id", "rxTime", "fromId", "fromNum", "toId", "portnum", "channel",
    # hops
    "hopStart", "hopLimit",
    # radio
    "rxSnr", "rxRssi",
    # device/telemetry
    "batteryLevel", "voltage", "airUtilTx", "channelUtilization", "uptimeSeconds",
    # environment
    "temperature", "humidity", "barometricPressure", "iaq", "co2", "gasResistance",
    # simple power metrics (non-INA3221)
    "pmVoltage", "pmCurrent",
    # position
    "latitude", "longitude", "altitude", "time", "PDOP", "groundSpeed", "groundTrack",
    "satsInView", "locationSource",
    # text
    "text",
    # LOCAL STATS (from telemetry.localStats)
    "numPacketsTx", "numPacketsRx", "numPacketsRxBad", "numOnlineNodes", "numTotalNodes",
    "numRxDupe", "numTxRelay", "numTxRelayCanceled", "heapTotalBytes", "heapFreeBytes",
    # NODEINFO (decoded.user)
    "userId", "userLongName", "userShortName", "hwModel", "macaddr", "publicKey",
    # INA3221
    "ina1Voltage", "ina1Current", "ina1Power", "ina1ShuntVoltage",
    "ina2Voltage", "ina2Current", "ina2Power", "ina2ShuntVoltage",
    "ina3Voltage", "ina3Current", "ina3Power", "ina3ShuntVoltage",
    # WAYPOINT
    "waypointId", "waypointLat", "waypointLon",
    "waypointName", "waypointDescription", "waypointIcon",
    "waypointLockedTo", "waypointExpire", "waypointExpireRaw",
    "waypointLatitudeI", "waypointLongitudeI",
    "waypointLatitudeRaw", "waypointLongitudeRaw",
    "waypointPayloadB64",
    # Routing
    "routingRequestId", "routingErrorReason", "isAck", "routingPayloadB64",
]

# ---------------- Row extractor ----------------

def extract_csv_row(packet_obj) -> dict:
    p = ensure_dict(packet_obj)

    decoded = _asdict(p.get("decoded", {}))
    telem   = _asdict(decoded.get("telemetry", {}))
    dev     = _asdict(telem.get("deviceMetrics", {}))
    env     = _asdict(telem.get("environmentMetrics", {}))
    pm      = _asdict(telem.get("powerMetrics", {}))
    lstats  = _asdict(telem.get("localStats", {}))
    user    = _asdict(decoded.get("user", {}))

    # payload fallbacks
    payload = _asdict(decoded.get("payload", {}))
    env2 = _asdict(payload.get("environmentMetrics", {}))
    dev2 = _asdict(payload.get("deviceMetrics", {}))
    pm2  = _asdict(payload.get("powerMetrics", {}))
    if not env: env = env2
    if not dev: dev = dev2
    if not pm:  pm  = pm2

    pos = _asdict(decoded.get("position", {}))

    # RSSI/SNR: prefer top-level, fallback to rxMetadata
    rxSnr  = p.get("rxSnr")
    rxRssi = p.get("rxRssi")
    if rxSnr is None or rxRssi is None:
        rxm = _first_of_list_or_dict(p.get("rxMetadata"))
        rxSnr  = rxSnr  if rxSnr  is not None else rxm.get("snr")
        rxRssi = rxRssi if rxRssi is not None else rxm.get("rssi")

    # humidity key variants
    humidity = env.get("humidity", env.get("relativeHumidity"))

    # simple power metrics (non-INA)
    pmVoltage = pm.get("voltage")
    pmCurrent = pm.get("current")

    ina = parse_ina3221(decoded)

    row = {
        # meta
        "timestamp": datetime.now().isoformat(),
        "id": p.get("id"),
        "rxTime": p.get("rxTime"),
        "fromId": p.get("fromId"),
        "fromNum": p.get("from"),
        "toId": p.get("toId"),
        "portnum": decoded.get("portnum"),
        "channel": p.get("channel") or decoded.get("channel"),

        # hops
        "hopStart": p.get("hopStart"),
        "hopLimit": p.get("hopLimit"),

        # radio
        "rxSnr": rxSnr,
        "rxRssi": rxRssi,

        # device/telemetry
        "batteryLevel": dev.get("batteryLevel"),
        "voltage": dev.get("voltage"),
        "airUtilTx": dev.get("airUtilTx"),
        "channelUtilization": dev.get("channelUtilization"),
        "uptimeSeconds": dev.get("uptimeSeconds"),

        # environment
        "temperature": env.get("temperature"),
        "humidity": humidity,
        "barometricPressure": env.get("barometricPressure"),
        "iaq": env.get("iaq"),
        "co2": env.get("co2"),
        "gasResistance": env.get("gasResistance"),

        # simple power metrics (non-INA)
        "pmVoltage": pmVoltage,
        "pmCurrent": pmCurrent,

        # position (+ locationSource)
        "latitude": pos.get("latitude"),
        "longitude": pos.get("longitude"),
        "altitude": pos.get("altitude"),
        "time": pos.get("time"),
        "PDOP": pos.get("PDOP"),
        "groundSpeed": pos.get("groundSpeed"),
        "groundTrack": pos.get("groundTrack"),
        "satsInView": pos.get("satsInView"),
        "locationSource": pos.get("locationSource"),

        # text
        "text": decoded.get("text"),

        # localStats
        "numPacketsTx": lstats.get("numPacketsTx"),
        "numPacketsRx": lstats.get("numPacketsRx"),
        "numPacketsRxBad": lstats.get("numPacketsRxBad"),
        "numOnlineNodes": lstats.get("numOnlineNodes"),
        "numTotalNodes": lstats.get("numTotalNodes"),
        "numRxDupe": lstats.get("numRxDupe"),
        "numTxRelay": lstats.get("numTxRelay"),
        "numTxRelayCanceled": lstats.get("numTxRelayCanceled"),
        "heapTotalBytes": lstats.get("heapTotalBytes"),
        "heapFreeBytes": lstats.get("heapFreeBytes"),

        # nodeinfo user
        "userId": user.get("id"),
        "userLongName": user.get("longName"),
        "userShortName": user.get("shortName"),
        "hwModel": user.get("hwModel"),
        "macaddr": user.get("macaddr"),
        "publicKey": user.get("publicKey"),

        # INA3221 merged
        **ina,
    }

    # WAYPOINT_APP → copy selected fields into CSV row
    if decoded.get("portnum") == "WAYPOINT_APP" or decoded.get("waypoint"):
        wrow = _extract_waypoint_fields(p)
        for k in (
            "waypointId","waypointLat","waypointLon","waypointName","waypointDescription",
            "waypointIcon","waypointLockedTo","waypointExpire","waypointExpireRaw",
            "waypointLatitudeI","waypointLongitudeI","waypointLatitudeRaw",
            "waypointLongitudeRaw","waypointPayloadB64"
        ):
            row[k] = wrow.get(k)

    return row

# ---------------- Logger ----------------

class MeshtasticDebugLogger:
    def __init__(self, host: str, outdir: str, csv_enabled: bool):
        self.host = host
        self.outdir = Path(outdir)
        self.outdir.mkdir(parents=True, exist_ok=True)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.jsonl_path = self.outdir / f"meshtastic_dump_{ts}.jsonl"
        self.log_file = open(self.jsonl_path, "a", encoding="utf-8")

        self.csv_enabled = csv_enabled
        self.csv_path = None
        self.csv_file = None
        self.csv_writer = None
        if self.csv_enabled:
            self.csv_path = self.outdir / f"meshtastic_flat_{ts}.csv"
            self.csv_file = open(self.csv_path, "a", newline="", encoding="utf-8")
            self.csv_writer = csv.DictWriter(self.csv_file, fieldnames=CSV_COLUMNS)
            self.csv_writer.writeheader()

        self.iface = None

    def connect(self):
        print(f"🔌 Connecting to Meshtastic over TCP {self.host}:4403 …")
        self.iface = meshtastic.tcp_interface.TCPInterface(hostname=self.host)
        pub.subscribe(self.on_packet, "meshtastic.receive")
        print(f"✅ Connected.\n- JSONL: {self.jsonl_path}")
        if self.csv_enabled:
            print(f"- CSV:   {self.csv_path}")
        print()

    def on_packet(self, packet=None, interface=None, **kwargs):
        try:
            if packet is None:
                return

            p_norm = ensure_dict(packet)

            # JSONL (bytes-safe)
            wrapped = {"timestamp": datetime.now().isoformat(), "packet": jsonable(packet)}
            self.log_file.write(json.dumps(wrapped, ensure_ascii=False) + "\n")
            self.log_file.flush()

            # CSV
            if self.csv_enabled:
                row = extract_csv_row(p_norm)
                self.csv_writer.writerow(row)
                self.csv_file.flush()

            # Console summary
            decoded = _asdict(p_norm.get("decoded", {}))
            port = decoded.get("portnum")
            print(f"[{wrapped['timestamp']}] port={port} from={p_norm.get('fromId') or p_norm.get('from')} to={p_norm.get('toId')}")
            if port == "TELEMETRY_APP":
                telem = _asdict(decoded.get("telemetry", {}))
                dev = _asdict(telem.get("deviceMetrics", {}))
                env = _asdict(telem.get("environmentMetrics", {}))
                hum = env.get("humidity", env.get("relativeHumidity"))
                print(f"  TELEM: Batt={dev.get('batteryLevel')}% V={dev.get('voltage')}V "
                      f"Temp={env.get('temperature')}°C Hum={hum}% Press={env.get('barometricPressure')}hPa")
            elif port == "POSITION_APP":
                pos = _asdict(decoded.get("position", {}))
                print(f"  POS: lat={pos.get('latitude')} lon={pos.get('longitude')} alt={pos.get('altitude')}")
            elif port == "NODEINFO_APP":
                user = _asdict(decoded.get("user", {}))
                print(f"  NODE: {user.get('longName')} ({user.get('shortName')}) {user.get('hwModel')} id={user.get('id')}")
            elif port == "WAYPOINT_APP":
                w = _extract_waypoint_fields(p_norm)
                print(f"  WPT: id={w.get('waypointId')} name={w.get('waypointName')} "
                      f"lat={w.get('waypointLat')} lon={w.get('waypointLon')} expire={w.get('waypointExpire')}")
                if self.csv_enabled:
                    print("  ↳ waypoint fields written to CSV")

        except Exception as e:
            print(f"[on_packet error] {e}")

    def run(self, duration: int = 0):
        start = time.time()
        while True:
            time.sleep(0.1)
            if duration and (time.time() - start > duration):
                break

    def close(self):
        print("🔚 Closing connection…")
        try:
            if self.iface:
                self.iface.close()
        except Exception:
            pass
        try:
            self.log_file.close()
        except Exception:
            pass
        if self.csv_enabled:
            try:
                self.csv_file.close()
            except Exception:
                pass
        print("✅ Done.")

# ---------------- CLI ----------------

def main():
    parser = argparse.ArgumentParser(description="Meshtastic TCP debug listener with JSONL + CSV (+telemetry layout).")
    parser.add_argument("--host", required=True, help="Node IP or hostname (default node port 4403)")
    parser.add_argument("--seconds", type=int, default=0, help="Run duration (0 = infinite)")
    parser.add_argument("--outdir", default="mesh_debug_logs", help="Output folder")
    parser.add_argument("--csv", action="store_true", help="Also write a flattened CSV alongside JSONL")
    args = parser.parse_args()

    logger = MeshtasticDebugLogger(args.host, args.outdir, csv_enabled=args.csv)
    try:
        logger.connect()
        logger.run(duration=args.seconds)
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user.")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        logger.close()

if __name__ == "__main__":
    sys.exit(main())
