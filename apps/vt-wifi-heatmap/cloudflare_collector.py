import csv
import re
import subprocess
import time
import uuid
from datetime import datetime, timezone

import requests


# -----------------------------
# CONFIG
# -----------------------------
file_name = input("Put a filename (must put .csv at the end): ")
OUTPUT_FILE = file_name

PING_HOST = "1.1.1.1"
PING_COUNT = 10

# Cloudflare's public speed-test endpoints.
DOWNLOAD_URL = "https://speed.cloudflare.com/__down"
UPLOAD_URL = "https://speed.cloudflare.com/__up"

# Keep these modest for a hackathon so you do not burn lots of bandwidth.
DOWNLOAD_BYTES = 10_000_000   # 10 MB
UPLOAD_BYTES = 5_000_000      # 5 MB


def safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def get_wifi_info():
    """
    Read the currently connected Wi-Fi interface using Windows netsh.
    Returns signal quality, estimated RSSI, SSID, BSSID, channel, radio type,
    receive link rate, transmit link rate, etc.
    """
    result = subprocess.run(
        ["netsh", "wlan", "show", "interfaces"],
        capture_output=True,
        text=True,
        errors="ignore"
    )

    if result.returncode != 0:
        raise RuntimeError("Could not run 'netsh wlan show interfaces'.")

    fields = {}
    for line in result.stdout.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        fields[key.strip().lower()] = value.strip()

    state = fields.get("state", "")
    if state.lower() != "connected":
        raise RuntimeError("Your laptop does not appear to be connected to Wi-Fi.")

    signal_text = fields.get("signal", "")
    match = re.search(r"(\d+)", signal_text)
    signal_pct = int(match.group(1)) if match else None

    # Microsoft defines WLAN signal quality as:
    # 0% -> -100 dBm, 100% -> -50 dBm, linear interpolation in between.
    estimated_rssi_dbm = (
        -100 + (signal_pct / 2.0)
        if signal_pct is not None
        else None
    )

    def num_field(name):
        value = fields.get(name)
        if value is None:
            return None
        m = re.search(r"-?\d+(?:\.\d+)?", value)
        return float(m.group(0)) if m else None

    return {
        "ssid": fields.get("ssid"),
        "bssid": fields.get("bssid"),
        "signal_pct": signal_pct,
        "estimated_rssi_dbm": round(estimated_rssi_dbm, 1)
        if estimated_rssi_dbm is not None else None,
        "band": fields.get("band"),
        "channel": fields.get("channel"),
        "radio_type": fields.get("radio type"),
        "rx_link_mbps": num_field("receive rate (mbps)"),
        "tx_link_mbps": num_field("transmit rate (mbps)"),
        "wifi_interface": fields.get("name"),
        "adapter_description": fields.get("description"),
    }


def ping_test(host=PING_HOST, count=PING_COUNT):
    """
    Uses the Windows ping command.
    latency_ms = mean RTT of received packets
    jitter_ms = mean absolute change between consecutive RTTs
    packet_loss_pct = percentage of packets lost
    """
    result = subprocess.run(
        ["ping", "-n", str(count), "-w", "1500", host],
        capture_output=True,
        text=True,
        errors="ignore"
    )

    output = result.stdout

    # Matches: time=12ms and time<1ms
    time_matches = re.findall(r"time[=<]\s*(\d+)\s*ms", output, flags=re.IGNORECASE)
    times = [float(x) for x in time_matches]

    loss_match = re.search(r"\((\d+)%\s*loss\)", output, flags=re.IGNORECASE)
    packet_loss_pct = float(loss_match.group(1)) if loss_match else None

    latency_ms = sum(times) / len(times) if times else None

    if len(times) >= 2:
        diffs = [abs(times[i] - times[i - 1]) for i in range(1, len(times))]
        jitter_ms = sum(diffs) / len(diffs)
    else:
        jitter_ms = None

    return {
        "ping_host": host,
        "latency_ms": round(latency_ms, 2) if latency_ms is not None else None,
        "jitter_ms": round(jitter_ms, 2) if jitter_ms is not None else None,
        "packet_loss_pct": packet_loss_pct,
        "ping_packets_received": len(times),
        "ping_packets_sent": count,
    }


def download_test():
    """
    Downloads a fixed number of bytes from Cloudflare and measures throughput.
    """
    params = {
        "bytes": DOWNLOAD_BYTES,
        "cachebust": uuid.uuid4().hex
    }

    start = time.perf_counter()
    total_bytes = 0

    with requests.get(
        DOWNLOAD_URL,
        params=params,
        stream=True,
        timeout=30
    ) as response:
        response.raise_for_status()

        for chunk in response.iter_content(chunk_size=256 * 1024):
            if chunk:
                total_bytes += len(chunk)

    elapsed = time.perf_counter() - start
    mbps = (total_bytes * 8) / elapsed / 1_000_000

    return round(mbps, 2)


def upload_test():
    """
    Uploads a modest in-memory payload to Cloudflare and measures throughput.
    """
    payload = b"0" * UPLOAD_BYTES

    start = time.perf_counter()

    response = requests.post(
        UPLOAD_URL,
        data=payload,
        headers={"Content-Type": "application/octet-stream"},
        timeout=30
    )
    response.raise_for_status()

    elapsed = time.perf_counter() - start
    mbps = (UPLOAD_BYTES * 8) / elapsed / 1_000_000

    return round(mbps, 2)


def collect_one_sample(location_name, latitude, longitude):
    timestamp = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

    wifi = get_wifi_info()
    ping = ping_test()

    try:
        download_mbps = download_test()
    except Exception as exc:
        print(f"  Download test failed: {exc}")
        download_mbps = None

    try:
        upload_mbps = upload_test()
    except Exception as exc:
        print(f"  Upload test failed: {exc}")
        upload_mbps = None

    row = {
        "test_id": str(uuid.uuid4()),
        "timestamp": timestamp,
        "location_name": location_name,
        "latitude": latitude,
        "longitude": longitude,
        "source": "real",

        **wifi,
        **ping,

        "download_mbps": download_mbps,
        "upload_mbps": upload_mbps,
    }

    return row


def append_row(row):
    fieldnames = list(row.keys())

    try:
        with open(OUTPUT_FILE, "r", newline="", encoding="utf-8") as f:
            file_exists = True
    except FileNotFoundError:
        file_exists = False

    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        writer.writerow(row)


def main():
    print("\nVT / Blacksburg Network Measurement Collector")
    print("--------------------------------------------")

    location_name = input(
        "Location name (example: Squires bus stop): "
    ).strip()

    latitude = input(
        "Latitude (optional; press Enter to skip): "
    ).strip()

    longitude = input(
        "Longitude (optional; press Enter to skip): "
    ).strip()

    latitude = safe_float(latitude)
    longitude = safe_float(longitude)

    while True:
        try:
            num_samples = int(input("How many samples? [10]: ") or "10")
            break
        except ValueError:
            print("Please enter a whole number.")

    while True:
        try:
            interval = int(
                input("Seconds between samples? [15]: ") or "15"
            )
            break
        except ValueError:
            print("Please enter a whole number.")

    print(f"\nWriting measurements to: {OUTPUT_FILE}")
    print("Stay in roughly the same physical position during this run.\n")

    for i in range(1, num_samples + 1):
        print(f"[{i}/{num_samples}] Measuring...")

        try:
            row = collect_one_sample(location_name, latitude, longitude)
            append_row(row)

            print(
                f"  Signal={row['signal_pct']}% "
                f"(~{row['estimated_rssi_dbm']} dBm) | "
                f"Down={row['download_mbps']} Mbps | "
                f"Up={row['upload_mbps']} Mbps | "
                f"Latency={row['latency_ms']} ms | "
                f"Jitter={row['jitter_ms']} ms | "
                f"Loss={row['packet_loss_pct']}%"
            )
        except Exception as exc:
            print(f"  Sample failed: {exc}")

        if i < num_samples:
            time.sleep(interval)

    print("\nDone.")
    print(f"Your data is in {OUTPUT_FILE}")
    print("Run this script again at another location; it will append new rows.")


if __name__ == "__main__":
    main()
