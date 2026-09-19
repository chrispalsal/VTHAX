import argparse
import shutil
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description="Start an iperf3 server")
    parser.add_argument("--port", type=int, default=5201)
    parser.add_argument(
        "--bind",
        default=None,
        help="Optional local IP address on which to listen"
    )
    args = parser.parse_args()

    iperf_path = shutil.which("iperf3")

    if not iperf_path:
        print("Error: iperf3 is not installed or not in PATH.")
        sys.exit(1)

    command = [
        iperf_path,
        "--server",
        "--port",
        str(args.port)
    ]

    if args.bind:
        command.extend(["--bind", args.bind])

    print(f"Starting iperf3 server on port {args.port}...")
    print("Press Ctrl+C to stop.")

    try:
        subprocess.run(command, check=True)
    except KeyboardInterrupt:
        print("\nServer stopped.")
    except subprocess.CalledProcessError as error:
        print(f"iperf3 exited with code {error.returncode}")


if __name__ == "__main__":
    main()