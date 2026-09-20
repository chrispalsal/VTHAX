#!/usr/bin/env python3
"""
eduroam collector for the VTHacks project (Windows first).

Checkpoint IDs MUST match zone_id values in the Databricks table
vt_connectivity.campus.zone_reference so the pipeline can join measurements
with GPS coordinates and building/floor metadata. Use --list-zones to see
all valid zone IDs.

Two speeds:
  light tick  every few seconds: connected-AP info (netsh) plus pings to the
              default gateway and to 1.1.1.1
  heavy test  short iperf3 runs (TCP up, TCP down, UDP 3 Mbit/s up), on demand
              or on a slow timer. It floods the link on purpose, so stand still.

Everything is written to local CSVs first (data/<device>_<session>_<table>.csv).
An optional --upload-url gets batched JSON POSTs, and failures just retry later.

While it runs, type in the same window:
    Enter            you are at the next checkpoint of the --route file
    some-id + Enter  you are at checkpoint some-id
    h + Enter        run a heavy iperf3 test now (stand still)
    q + Enter        quit

Route file: one checkpoint ID per line (blank lines and # comments ignored).
Use --list-zones to see all valid checkpoint IDs (from zone_reference).

Examples:
    python collector.py --device win1 --mode walk --route routes\\torg_f1.txt
    python collector.py --device win1 --mode stand
    python collector.py --device win2 --server 10.110.205.57 --port 5202
"""
import argparse
import csv
import datetime as dt
import json
import platform
import queue
import random
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

IS_WIN = platform.system() == "Windows"
DEFAULT_SERVER = "10.110.205.57"  # private address: only reachable on the network it lives on

# All valid zone_ids from vt_connectivity.campus.zone_reference (45 zones).
# The collector validates checkpoint IDs against this list.
# Update this list if new zones are added to zone_reference.
ZONES = {
    # Goodwin Hall indoor (12)
    "goodwin_f1_lobby", "goodwin_f1_lab1", "goodwin_f1_hall", "goodwin_f1_lab2",
    "goodwin_f2_aero", "goodwin_f2_class", "goodwin_f2_hall",
    "goodwin_f3_office", "goodwin_f3_conf", "goodwin_f3_stairs",
    "goodwin_f4_study", "goodwin_f4_corner",
    # Outdoor campus zones (33)
    "drillfield_c", "drillfield_e", "drillfield_n", "drillfield_s", "drillfield_w",
    "burruss_front", "burruss_steps",
    "newman_front", "newman_steps",
    "squires_front", "squires_food",
    "torg_front", "torg_bridge",
    "goodwin_front", "goodwin_lot",
    "bus_drillfield", "bus_squires",
    "path_drill_burr", "path_drill_torg", "path_torg_squires",
    "stone_path", "hill_top",
    "ag_quad", "eng_quad", "lower_quad", "upper_quad_n", "upper_quad_s",
    "slusher_quad", "payne_entrance", "owens_front",
    "airport_area", "dod_stadium", "vet_med",
    # Bus stops (real data collected)
    "maroon_bay_1", "maroon_bay_5",
}

FIELDS = {
    "measurements": [
        "ts_utc", "device_id", "session_id", "ssid", "bssid", "signal_pct", "rssi_est_dbm",
        "channel", "band", "radio_type", "rx_rate_mbps", "tx_rate_mbps", "gw_ip",
        "gw_latency_ms", "gw_jitter_ms", "gw_loss_pct", "inet_latency_ms", "inet_jitter_ms",
        "inet_loss_pct", "last_checkpoint", "next_checkpoint", "note",
    ],
    "scans": [
        "ts_utc", "device_id", "session_id", "ssid", "bssid", "signal_pct", "rssi_est_dbm",
        "channel", "band", "radio_type",
    ],
    "iperf": [
        "ts_utc", "device_id", "session_id", "test", "server_ip", "server_port", "seconds",
        "ok", "mbps", "retransmits", "jitter_ms", "lost_pct", "lost_packets", "packets",
        "error", "bssid", "signal_pct", "last_checkpoint",
    ],
    "taps": ["ts_utc", "device_id", "session_id", "checkpoint_id", "source"],
}


# ---------------------------------------------------------------- helpers
def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds")


def sh(cmd, timeout=10):
    return subprocess.run(
        cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout
    ).stdout


def kv(line):
    """'  Key : value' -> ('Key', 'value'). Splits on the FIRST colon (MAC addresses contain more)."""
    if ":" not in line:
        return None, None
    k, v = line.split(":", 1)
    return k.strip(), v.strip()


def to_int(s):
    m = re.search(r"\d+", s or "")
    return int(m.group()) if m else None


def to_float(s):
    m = re.search(r"\d+(?:\.\d+)?", s or "")
    return float(m.group()) if m else None


def sig_to_dbm(pct):
    """Windows only reports quality %. Documented mapping: dBm ~ pct/2 - 100 (approximate,
    saturates near -50 dBm)."""
    return None if pct is None else round(pct / 2 - 100, 1)


# ---------------------------------------------------------------- Wi-Fi (netsh)
def _wifi_from_block(b):
    pct = to_int(b.get("Signal"))
    ch = to_int(b.get("Channel"))
    band = b.get("Band") or (("2.4 GHz" if ch <= 14 else "5 GHz") if ch else None)  # inferred if no Band line
    bssid = b.get("AP BSSID") or b.get("BSSID")
    return {
        "ssid": b.get("SSID"),
        "bssid": bssid.lower() if bssid else None,
        "signal_pct": pct,
        "rssi_est_dbm": sig_to_dbm(pct),
        "channel": ch,
        "band": band,
        "radio_type": b.get("Radio type"),
        "rx_rate_mbps": to_float(b.get("Receive rate (Mbps)")),
        "tx_rate_mbps": to_float(b.get("Transmit rate (Mbps)")),
    }


def parse_interfaces(text):
    blocks, cur = [], None
    for line in text.splitlines():
        k, v = kv(line)
        if k is None:
            continue
        if k == "Name":
            cur = {}
            blocks.append(cur)
        if cur is not None and k not in cur:
            cur[k] = v
    for b in blocks:
        if b.get("State", "").lower() == "connected":
            return _wifi_from_block(b)
    return {"note": "not connected" if blocks else "no Wi-Fi interface found"}


def parse_networks(text):
    out, ssid, cur = [], None, None
    for line in text.splitlines():
        k, v = kv(line)
        if k is None:
            continue
        if re.fullmatch(r"SSID\s+\d+", k):
            ssid, cur = v, None
        elif re.fullmatch(r"BSSID\s+\d+", k):
            cur = {"ssid": ssid, "bssid": v.lower()}
            out.append(cur)
        elif cur is not None:
            if k == "Signal":
                cur["signal_pct"] = to_int(v)
            elif k == "Channel":
                cur["channel"] = to_int(v)
            elif k == "Band":
                cur["band"] = v
            elif k == "Radio type":
                cur["radio_type"] = v
    for n in out:
        if "band" not in n and n.get("channel"):
            n["band"] = "2.4 GHz" if n["channel"] <= 14 else "5 GHz"
    return out


def wifi_info():
    if not IS_WIN:
        return {"note": "netsh unavailable (not Windows)"}
    try:
        return parse_interfaces(sh(["netsh", "wlan", "show", "interfaces"]))
    except Exception as e:
        return {"note": f"netsh error: {e}"}


def scan_networks():
    """Windows returns its cached scan list, which refreshes only about once a minute,
    so scanning more often than ~30 s mostly repeats rows."""
    if not IS_WIN:
        return []
    try:
        return parse_networks(sh(["netsh", "wlan", "show", "networks", "mode=bssid"], timeout=15))
    except Exception:
        return []


# ---------------------------------------------------------------- gateway + ping
def parse_default_gateway(text):
    best = None
    for line in text.splitlines():
        m = re.match(r"\s*0\.0\.0\.0\s+0\.0\.0\.0\s+(\d+\.\d+\.\d+\.\d+)\s+\S+\s+(\d+)", line)
        if m:
            cand = (int(m.group(2)), m.group(1))
            best = cand if best is None or cand < best else best
    return best[1] if best else None


def default_gateway():
    try:
        if IS_WIN:
            return parse_default_gateway(sh(["route", "print", "-4", "0.0.0.0"]))
        m = re.search(r"default via (\S+)", sh(["ip", "route", "show", "default"]))
        return m.group(1) if m else None
    except Exception:
        return None


def parse_ping(text, count):
    rtts = [float(x) for x in re.findall(r"time[=<]\s*([\d.]+)\s*ms", text)]  # English Windows/Linux output
    diffs = [abs(b - a) for a, b in zip(rtts, rtts[1:])]
    return {
        "avg": round(sum(rtts) / len(rtts), 2) if rtts else None,
        "jitter": round(sum(diffs) / len(diffs), 2) if diffs else None,
        "loss": round((count - len(rtts)) / count * 100, 1),
    }


def ping(host, count):
    if count <= 0 or not host:
        return {}
    cmd = ["ping", "-n", str(count), "-w", "1000", host] if IS_WIN else ["ping", "-c", str(count), "-W", "1", host]
    try:
        return parse_ping(sh(cmd, timeout=count * 1.5 + 6), count)
    except Exception:
        return {"avg": None, "jitter": None, "loss": 100.0}


# ---------------------------------------------------------------- iperf3
def _iperf_once(server, port, mode, secs):
    args = ["iperf3", "-c", server, "-p", str(port), "-t", str(secs), "-J"]
    if mode == "tcp_down":
        args += ["-R"]
    if mode.startswith("tcp"):
        args += ["-O", "1"]  # omit the first second so TCP slow start doesn't drag the number
    if mode == "udp_up":
        args += ["-u", "-b", "3M"]  # ~ one Teams video call worth of traffic
    res = {"test": mode, "seconds": secs, "ok": False}
    try:
        p = subprocess.run(args, capture_output=True, text=True, timeout=secs + 12)
    except FileNotFoundError:
        res["error"] = "iperf3 not found (install it and add it to PATH)"
        return res
    except subprocess.TimeoutExpired:
        res["error"] = "timeout"
        return res
    try:
        data = json.loads(p.stdout)
    except ValueError:
        res["error"] = ((p.stderr or p.stdout or "").strip()[:200]) or f"exit code {p.returncode}"
        return res
    if data.get("error"):
        res["error"] = str(data["error"])[:200]
        return res
    end = data.get("end", {})
    if mode.startswith("tcp"):
        # sum_received = what the RECEIVER got. Default direction is client->server (upload);
        # with -R the client is the receiver (download).
        recv, sent = end.get("sum_received", {}), end.get("sum_sent", {})
        if "bits_per_second" not in recv:
            res["error"] = "no sum_received in iperf3 output"
            return res
        res["mbps"] = round(recv["bits_per_second"] / 1e6, 2)
        res["retransmits"] = sent.get("retransmits")
    else:
        s = end.get("sum") or end.get("sum_received") or {}
        if "bits_per_second" not in s:
            res["error"] = "no UDP summary in iperf3 output"
            return res
        res["mbps"] = round(s["bits_per_second"] / 1e6, 2)
        res["jitter_ms"] = round(s.get("jitter_ms", 0), 3)
        res["lost_pct"] = round(s.get("lost_percent", 0), 2)
        res["lost_packets"] = s.get("lost_packets")
        res["packets"] = s.get("packets")
    res["ok"] = True
    return res


def run_iperf(server, port, mode, secs, retries=2):
    for _ in range(retries + 1):
        r = _iperf_once(server, port, mode, secs)
        if r["ok"] or "busy" not in (r.get("error") or "").lower():
            return r
        time.sleep(random.uniform(1, 4))  # another laptop is testing; the server takes one at a time
    return r


# ---------------------------------------------------------------- storage + upload
class Uploader:
    def __init__(self, url, device, session, cap=5000):
        self.url, self.device, self.session, self.cap = url, device, session, cap
        self.pending, self.lock = {}, threading.Lock()

    def add(self, table, row):
        with self.lock:
            q = self.pending.setdefault(table, [])
            q.append(row)
            if len(q) > self.cap:
                del q[0]  # still safe on disk

    def flush(self):
        with self.lock:
            batch = {t: list(r) for t, r in self.pending.items() if r}
        for table, rows in batch.items():
            body = json.dumps(
                {"device_id": self.device, "session_id": self.session, "table": table, "rows": rows}
            ).encode()
            req = urllib.request.Request(
                self.url, data=body, headers={"Content-Type": "application/json"}, method="POST"
            )
            try:
                with urllib.request.urlopen(req, timeout=5) as resp:
                    if resp.status >= 300:
                        raise RuntimeError(f"HTTP {resp.status}")
            except Exception as e:
                print(f"[upload] {table}: failed ({e}); will retry")
                continue
            with self.lock:
                del self.pending[table][: len(rows)]


class Table:
    def __init__(self, name, path, uploader):
        self.name, self.uploader, self.lock, self.count = name, uploader, threading.Lock(), 0
        self.f = open(path, "w", newline="", encoding="utf-8")
        self.w = csv.DictWriter(self.f, fieldnames=FIELDS[name], extrasaction="ignore")
        self.w.writeheader()
        self.f.flush()

    def add(self, row):
        with self.lock:
            self.w.writerow(row)
            self.f.flush()
            self.count += 1
        if self.uploader:
            self.uploader.add(self.name, row)


class State:
    def __init__(self, route):
        self.lock, self.route, self.idx, self.last = threading.Lock(), route, 0, None

    def next_cp(self):
        return self.route[self.idx] if self.idx < len(self.route) else None


def load_route(path):
    if not path:
        return []
    zones = [l.strip() for l in Path(path).read_text(encoding="utf-8").splitlines()
             if l.strip() and not l.strip().startswith("#")]
    invalid = [z for z in zones if z not in ZONES]
    if invalid:
        print(f"[warn] route file has {len(invalid)} unknown zone_id(s): {', '.join(invalid)}")
        print(f"[warn] valid zones are in zone_reference. Use --list-zones to see them all.")
    return zones


# ---------------------------------------------------------------- taps (keyboard)
def tap_listener(cfg, state, tables, cmdq):
    for line in sys.stdin:
        text = line.strip()
        low = text.lower()
        if low == "q":
            cmdq.put("quit")
            return
        if low == "h":
            cmdq.put("heavy")
            continue
        with state.lock:
            if text == "":
                cp = state.next_cp()
                if cp is None:
                    print("[tap] no next checkpoint (give --route, or type an ID and press Enter)")
                    continue
                state.idx += 1
                source = "next"
            else:
                cp, source = text, "typed"
                if cp not in ZONES:
                    print(f"[warn] '{cp}' is not a valid zone_id. Use --list-zones to see valid IDs.")
                    continue
                if cp in state.route:
                    state.idx = state.route.index(cp) + 1
            state.last = cp
            nxt = state.next_cp()
        tables["taps"].add({"ts_utc": utc_now(), "device_id": cfg.device, "session_id": cfg.session,
                            "checkpoint_id": cp, "source": source})
        print(f"[tap] at {cp}  (next: {nxt or '-'})")


# ---------------------------------------------------------------- ticks
def light_tick(cfg, state, tables, gw):
    ts = utc_now()
    wifi = wifi_info()
    g, i = {}, {}
    if cfg.pings > 0:
        with ThreadPoolExecutor(2) as ex:  # both pings at once so a tick stays short
            fg = ex.submit(ping, gw, cfg.pings)
            fi = ex.submit(ping, cfg.inet_target, cfg.pings)
            g, i = fg.result(), fi.result()
    with state.lock:
        last, nxt = state.last, state.next_cp()
    note = wifi.get("note") or ("no gateway found" if not gw else None)
    row = {
        "ts_utc": ts, "device_id": cfg.device, "session_id": cfg.session,
        **{k: wifi.get(k) for k in ("ssid", "bssid", "signal_pct", "rssi_est_dbm", "channel", "band",
                                    "radio_type", "rx_rate_mbps", "tx_rate_mbps")},
        "gw_ip": gw, "gw_latency_ms": g.get("avg"), "gw_jitter_ms": g.get("jitter"),
        "gw_loss_pct": g.get("loss"), "inet_latency_ms": i.get("avg"),
        "inet_jitter_ms": i.get("jitter"), "inet_loss_pct": i.get("loss"),
        "last_checkpoint": last, "next_checkpoint": nxt, "note": note,
    }
    tables["measurements"].add(row)
    print(f"{ts[11:19]} {wifi.get('ssid')} {wifi.get('rssi_est_dbm')}dBm ch{wifi.get('channel')} "
          f"| gw {g.get('avg')}ms/{g.get('loss')}% | net {i.get('avg')}ms/{i.get('loss')}% "
          f"| at {last or '-'} -> {nxt or '-'}" + (f" | {note}" if note else ""))
    return wifi


def scan_tick(cfg, tables):
    nets = scan_networks()
    keep = [n for n in nets if (n.get("ssid") or "").lower() in cfg.scan_ssids]
    ts = utc_now()
    for n in keep:
        tables["scans"].add({
            "ts_utc": ts, "device_id": cfg.device, "session_id": cfg.session, "ssid": n.get("ssid"),
            "bssid": n.get("bssid"), "signal_pct": n.get("signal_pct"),
            "rssi_est_dbm": sig_to_dbm(n.get("signal_pct")), "channel": n.get("channel"),
            "band": n.get("band"), "radio_type": n.get("radio_type"),
        })
    print(f"[scan] stored {len(keep)} {'/'.join(sorted(cfg.scan_ssids))} APs; "
          f"{len(nets) - len(keep)} other networks ignored (not stored)")


def heavy_test(cfg, state, tables):
    if cfg.no_iperf:
        print("[iperf] disabled (--no-iperf)")
        return
    snap = wifi_info()
    with state.lock:
        last = state.last
    print(f"[iperf] heavy test to {cfg.server}:{cfg.port} - stand still for ~{2 * (cfg.tcp_secs + 1) + cfg.udp_secs}s")
    for mode, secs in (("tcp_up", cfg.tcp_secs), ("tcp_down", cfg.tcp_secs), ("udp_up", cfg.udp_secs)):
        ts = utc_now()
        r = run_iperf(cfg.server, cfg.port, mode, secs)
        tables["iperf"].add({
            "ts_utc": ts, "device_id": cfg.device, "session_id": cfg.session, "server_ip": cfg.server,
            "server_port": cfg.port, "bssid": snap.get("bssid"), "signal_pct": snap.get("signal_pct"),
            "last_checkpoint": last, **r,
        })
        if r["ok"]:
            extra = f", jitter {r.get('jitter_ms')} ms, loss {r.get('lost_pct')}%" if mode == "udp_up" else ""
            print(f"[iperf] {mode}: {r['mbps']} Mbit/s{extra}")
        else:
            print(f"[iperf] {mode} FAILED: {r.get('error')} (skipping the rest of this round)")
            break


# ---------------------------------------------------------------- main
def parse_args(argv=None):
    p = argparse.ArgumentParser(description="eduroam collector", epilog=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--device", default=socket.gethostname(), help="short name, e.g. win1")
    p.add_argument("--server", default=DEFAULT_SERVER, help="iperf3 server IP")
    p.add_argument("--port", type=int, default=5201, help="iperf3 port (use one port per laptop)")
    p.add_argument("--inet-target", default="1.1.1.1")
    p.add_argument("--mode", choices=["walk", "stand"], default="walk",
                   help="walk: tick 2 s, 1 ping, no timed iperf; stand: tick 5 s, 4 pings, iperf every 5 min")
    p.add_argument("--interval", type=float, help="seconds between light ticks (overrides mode)")
    p.add_argument("--pings", type=int, help="pings per host per tick (overrides mode)")
    p.add_argument("--scan-every", type=float, default=30)
    p.add_argument("--scan-ssids", default="eduroam", help="only these SSIDs are stored from scans")
    p.add_argument("--heavy-every", type=float, help="seconds between timed iperf3 rounds, 0 = never")
    p.add_argument("--tcp-secs", type=int, default=3)
    p.add_argument("--udp-secs", type=int, default=5)
    p.add_argument("--no-iperf", action="store_true")
    p.add_argument("--route", help="text file, one checkpoint ID per line")
    p.add_argument("--out", default="data")
    p.add_argument("--upload-url", help="POST batched rows here as JSON (optional)")
    p.add_argument("--upload-every", type=float, default=60)
    p.add_argument("--list-zones", action="store_true", help="print all valid zone_ids and exit")
    a = p.parse_args(argv)
    d_interval, d_pings, d_heavy = {"walk": (2.0, 1, 0.0), "stand": (5.0, 4, 300.0)}[a.mode]
    a.interval = d_interval if a.interval is None else a.interval
    a.pings = d_pings if a.pings is None else a.pings
    a.heavy_every = d_heavy if a.heavy_every is None else a.heavy_every
    a.session = time.strftime("%Y%m%d-%H%M%S")
    a.scan_ssids = {s.strip().lower() for s in a.scan_ssids.split(",") if s.strip()}
    return a


def main(argv=None):
    cfg = parse_args(argv)
    if getattr(cfg, "list_zones", False):
        print(f"Valid zone_ids ({len(ZONES)} total):")
        indoor = sorted(z for z in ZONES if re.match(r"goodwin_f\d", z))
        outdoor = sorted(z for z in ZONES if z not in indoor)
        print("\n  Indoor (Goodwin Hall):")
        for z in indoor:
            print(f"    {z}")
        print("\n  Outdoor (campus-wide):")
        for z in outdoor:
            print(f"    {z}")
        print("\nUse these IDs in route files or type them during collection.")
        return
    if not IS_WIN:
        print("[warn] not Windows: Wi-Fi fields will be empty (fine for testing the loop only)")
    if not cfg.no_iperf and not shutil.which("iperf3"):
        print("[warn] iperf3 not found on PATH: heavy tests will log failures")
    out = Path(cfg.out)
    out.mkdir(parents=True, exist_ok=True)
    uploader = Uploader(cfg.upload_url, cfg.device, cfg.session) if cfg.upload_url else None
    tables = {n: Table(n, out / f"{cfg.device}_{cfg.session}_{n}.csv", uploader) for n in FIELDS}
    state = State(load_route(cfg.route))
    cmdq = queue.Queue()
    threading.Thread(target=tap_listener, args=(cfg, state, tables, cmdq), daemon=True).start()

    first = wifi_info()
    if first.get("ssid") and first["ssid"].lower() not in cfg.scan_ssids:
        print(f"[warn] connected to '{first['ssid']}', not eduroam - readings won't be comparable")
    print(f"session {cfg.session}, device {cfg.device}, mode {cfg.mode}, tick {cfg.interval}s, "
          f"{cfg.pings} ping(s)/host. Files in {out}/")
    print("Enter = next checkpoint | ID + Enter = at ID | h = heavy iperf3 here | q = quit")

    gw, gw_checked = default_gateway(), time.monotonic()
    now = time.monotonic()
    next_tick, next_scan = now, now
    next_heavy = now + cfg.heavy_every if cfg.heavy_every else None
    next_upload = now + cfg.upload_every
    try:
        while True:
            if time.monotonic() - gw_checked > 60:
                gw, gw_checked = default_gateway(), time.monotonic()
            light_tick(cfg, state, tables, gw)
            if time.monotonic() >= next_scan:
                scan_tick(cfg, tables)
                next_scan = time.monotonic() + cfg.scan_every
            if next_heavy and time.monotonic() >= next_heavy:
                heavy_test(cfg, state, tables)
                next_heavy = time.monotonic() + cfg.heavy_every
            if uploader and time.monotonic() >= next_upload:
                uploader.flush()
                next_upload = time.monotonic() + cfg.upload_every
            next_tick += cfg.interval
            next_tick = max(next_tick, time.monotonic())  # tick overran: don't burst to catch up
            while True:
                try:
                    cmd = cmdq.get(timeout=max(0.0, next_tick - time.monotonic()))
                except queue.Empty:
                    break
                if cmd == "quit":
                    raise KeyboardInterrupt
                if cmd == "heavy":
                    heavy_test(cfg, state, tables)
                    next_tick = max(next_tick, time.monotonic())
    except KeyboardInterrupt:
        pass
    finally:
        if uploader:
            uploader.flush()
        print("\nstopped. rows written: " + ", ".join(f"{n}={t.count}" for n, t in tables.items()))


if __name__ == "__main__":
    main()
