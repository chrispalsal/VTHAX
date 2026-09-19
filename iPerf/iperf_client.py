import argparse
import datetime
import json
import shutil
import subprocess
import sys
from pathlib import Path


def run_iperf(server, port, duration, reverse=False, udp=False, bandwidth="10M"):
    iperf_path = shutil.which("iperf3")

    if not iperf_path:
        raise RuntimeError("iperf3 is not installed or not in PATH.")

    command = [
        iperf_path,
        "--client",
        server,
        "--port",
        str(port),
        "--time",
        str(duration),
        "--json"
    ]

    if reverse:
        command.append("--reverse")

    if udp:
        command.extend([
            "--udp",
            "--bitrate",
            bandwidth
        ])

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False
    )

    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "iperf3 test failed")

    return json.loads(result.stdout)


def summarize_tcp(result):
    end = result["end"]

    sent = end.get("sum_sent", {})
    received = end.get("sum_received", {})

    return {
        "sent_mbps": sent.get("bits_per_second", 0) / 1_000_000,
        "received_mbps": received.get("bits_per_second", 0) / 1_000_000,
        "retransmits": sent.get("retransmits")
    }


def summarize_udp(result):
    summary = result["end"].get("sum", {})

    return {
        "throughput_mbps": summary.get("bits_per_second", 0) / 1_000_000,
        "jitter_ms": summary.get("jitter_ms"),
        "lost_packets": summary.get("lost_packets"),
        "total_packets": summary.get("packets"),
        "packet_loss_percent": summary.get("lost_percent")
    }


def main():
    parser = argparse.ArgumentParser(description="Run an iperf3 network test")
    parser.add_argument("server", help="IP address or hostname of the server")
    parser.add_argument("--port", type=int, default=5201)
    parser.add_argument("--duration", type=int, default=10)
    parser.add_argument("--reverse", action="store_true")
    parser.add_argument("--udp", action="store_true")
    parser.add_argument("--bandwidth", default="10M")
    parser.add_argument("--output", default="iperf_result.json")
    args = parser.parse_args()

    try:
        result = run_iperf(
            server=args.server,
            port=args.port,
            duration=args.duration,
            reverse=args.reverse,
            udp=args.udp,
            bandwidth=args.bandwidth
        )
    except RuntimeError as error:
        print(f"Test failed: {error}")
        sys.exit(1)

    record = {
        "measured_at": datetime.datetime.now(
            datetime.timezone.utc
        ).isoformat(),
        "server": args.server,
        "port": args.port,
        "direction": "download" if args.reverse else "upload",
        "protocol": "UDP" if args.udp else "TCP",
        "summary": (
            summarize_udp(result)
            if args.udp
            else summarize_tcp(result)
        ),
        "iperf_result": result
    }

    output_path = Path(args.output)
    output_path.write_text(json.dumps(record, indent=2), encoding="utf-8")

    print(json.dumps(record["summary"], indent=2))
    print(f"Complete result saved to {output_path}")


if __name__ == "__main__":
    main()