import subprocess
import json
import datetime

# --- Configuration ---
# Your CS lead must run `iperf3 -s` on this server machine
IPERF_SERVER_IP = "10.110.205.57"  # Replace with your actual server IP/Domain
IPERF_SERVER_PORT = "5201"

def run_iperf_test():
    print(f"📡 Connecting to iPerf3 server at {IPERF_SERVER_IP}...")
    
    try:
        # Run iPerf3 client (-c), 5 second duration (-t 5), output as JSON (-J)
        # Add '-u' to this command if you specifically want to test UDP (Zoom/Teams traffic)
        command = ["iperf3", "-c", IPERF_SERVER_IP, "-p", IPERF_SERVER_PORT, "-t", "5", "-J"]
        
        # Execute the command and capture the output
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        
        # Parse the JSON output provided by iPerf3
        iperf_data = json.loads(result.stdout)
        
        # Extract the key metrics from the "end" summary object
        download_bps = iperf_data['end']['sum_received']['bits_per_second']
        upload_bps = iperf_data['end']['sum_sent']['bits_per_second']
        
        # Convert bits per second to Megabits per second (Mbps)
        download_mbps = download_bps / 1_000_000
        upload_mbps = upload_bps / 1_000_000
        
        return {
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "download_mbps": round(download_mbps, 2),
            "upload_mbps": round(upload_mbps, 2),
            "protocol": "TCP",
            "server_pinged": IPERF_SERVER_IP
        }

    except subprocess.CalledProcessError as e:
        print("❌ iPerf3 execution failed. Is the server running and reachable?")
        # With -J, iperf3 reports errors as JSON on stdout, so stderr is often empty
        print(f"Error details: {e.stderr or e.stdout}")
        return None
    except FileNotFoundError:
        print("❌ iPerf3 binary not found. Participants must install iPerf3 on their OS first.")
        return None

if __name__ == "__main__":
    payload = run_iperf_test()
    if payload:
        print("\n📊 iPerf3 Network Metrics:")
        print(json.dumps(payload, indent=2))
        
        # Next step: Merge this dictionary with the BSSID/Hardware metrics script 
        # and POST it to your database.