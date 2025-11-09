#!/usr/bin/env python3
"""
Meshtastic TCP full debug listener + JSONL (bytes-safe) + CSV (flattened)
- Handles packets that may arrive as dict, str (JSON), bytes, or proto objects
"""

import argparse, sys, time, json, base64, csv
from datetime import datetime
from pathlib import Path

import meshtastic
import meshtastic.tcp_interface
from pubsub import pub

# ------------ JSONL helpers (bytes-safe) ------------
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

def pretty(obj):
    try:
        return json.dumps(jsonable(obj), indent=2, ensure_ascii=False)
    except Exception:
        return str(obj)

# ------------ Packet normalization ------------
def ensure_dict(p):
    """Return a dict for any input packet."""
    if isinstance(p, dict):
        return p
    if isinstance(p, (bytes, bytearray)):
        # Try to decode as UTF-8 JSON; otherwise store base64
        try:
            s = bytes(p).decode("utf-8", errors="strict")
            try:
                return json.loads(s)  # JSON string packet
            except Exception:
                return {"_str": s}
        except Exception:
            return {"_bytes_b64": base64.b64encode(bytes(p)).decode("ascii")}
    if isinstance(p, str):
        # Try JSON load; else wrap
        try:
            return json.loads(p)
        except Exception:
            return {"_str": p}
    if hasattr(p, "__dict__"):
        return dict(p.__dict__)
    # Last resort
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
        try:
            s = bytes(x).decode("utf-8")
            return json.loads(s) if s.startswith("{") else {"_str": s}
        except Exception:
            return {"_bytes_b64": base64.b64encode(bytes(x)).decode("ascii")}
    try:
        # try JSON-ifying; if it becomes a string, wrap
        j = json.loads(json.dumps(x, default=str))
        return j if isinstance(j, dict) else {"_repr": str(x)}
    except Exception:
        return {"_repr": str(x)}

def _first_of_list_or_dict(x):
    if isinstance(x, list) and x:
        return _asdict(x[0])
    return _asdict(x or {})

# ------------ CSV helpers ------------
CSV_COLUMNS = [
    # meta
    "timestamp", "fromId", "toId", "portnum", "channel",
    # hops
    "hopStart", "hopLimit",
    # radio
    "rxSnr", "rxRssi",
    # device/telemetry
    "batteryLevel", "voltage", "airUtilTx", "channelUtilization",
    # environment
    "temperature", "humidity", "barometricPressure", "iaq", "co2", "gasResistance",
    # position
    "latitude", "longitude", "altitude", "time", "PDOP", "groundSpeed",
    # text
    "text",
]

def extract_csv_row(packet_obj) -> dict:
    p = ensure_dict(packet_obj)
    decoded = _asdict(p.get("decoded", {}))
    payload = _asdict(decoded.get("payload", {}))
    env = _asdict(payload.get("environmentMetrics", {}))
    pos = _asdict(decoded.get("position", {}))
    rxm = _first_of_list_or_dict(p.get("rxMetadata"))

    humidity = env.get("humidity", env.get("relativeHumidity"))

    row = {
        "timestamp": datetime.now().isoformat(),
        "fromId": p.get("fromId"),
        "toId": p.get("toId"),
        "portnum": decoded.get("portnum"),
        "channel": decoded.get("channel"),
        "hopStart": p.get("hopStart"),
        "hopLimit": p.get("hopLimit"),
        "rxSnr": rxm.get("snr"),
        "rxRssi": rxm.get("rssi"),
        "batteryLevel": payload.get("batteryLevel"),
        "voltage": payload.get("voltage"),
        "airUtilTx": payload.get("airUtilTx"),
        "channelUtilization": payload.get("channelUtilization"),
        "temperature": env.get("temperature"),
        "humidity": humidity,
        "barometricPressure": env.get("barometricPressure"),
        "iaq": env.get("iaq"),
        "co2": env.get("co2"),
        "gasResistance": env.get("gasResistance"),
        "latitude": pos.get("latitude"),
        "longitude": pos.get("longitude"),
        "altitude": pos.get("altitude"),
        "time": pos.get("time"),
        "PDOP": pos.get("PDOP"),
        "groundSpeed": pos.get("groundSpeed"),
        "text": decoded.get("text"),
    }
    return row

# ------------ Logger ------------
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

            # Normalize for CSV/console
            p_norm = ensure_dict(packet)

            # JSONL (bytes-safe, lossless)
            wrapped = {"timestamp": datetime.now().isoformat(), "packet": jsonable(packet)}
            self.log_file.write(json.dumps(wrapped, ensure_ascii=False) + "\n")
            self.log_file.flush()

            # CSV (flattened)
            if self.csv_enabled:
                row = extract_csv_row(p_norm)
                self.csv_writer.writerow(row)
                self.csv_file.flush()

            # Console summary
            decoded = _asdict(p_norm.get("decoded", {}))
            port = decoded.get("portnum")
            print(f"[{wrapped['timestamp']}] port={port} from={p_norm.get('fromId')} to={p_norm.get('toId')}")

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

# ------------ CLI ------------
def main():
    parser = argparse.ArgumentParser(description="Meshtastic TCP debug listener with JSONL + optional CSV.")
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
