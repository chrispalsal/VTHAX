# iPerf diagnostic collector

This project can keep iPerf3 results in a local SQLite database while retaining
a JSON copy on each test device. It uses only the Python standard library.

## Start the collector

Set a token before exposing the collector to other devices:

```powershell
$env:DIAGNOSTICS_API_TOKEN = "replace-with-a-long-random-value"
python -m backend.server --bind 0.0.0.0 --port 8080
```

The database is created at `backend/data/diagnostics.sqlite3`. Allow inbound TCP
port 8080 through the collector machine's firewall only on the network being
used for the study. Plain HTTP is appropriate only on a trusted, isolated test
network; use an HTTPS reverse proxy before collecting over the internet.

## Run a test and store it

From a participant device with Python and iPerf3 installed:

```powershell
$env:DIAGNOSTICS_API_TOKEN = "replace-with-a-long-random-value"
python iPerf/iperf_client.py 10.0.0.10 `
  --collector-url http://10.0.0.10:8080 `
  --participant-id participant-001
```

Use a pseudonymous participant ID rather than a name, email address, phone
number, or hardware identifier. Omit `--participant-id` if records do not need
to be linked. The full raw iPerf JSON may contain source/destination IP
addresses, so define a retention period and restrict access to the database.

If an upload fails, the complete result remains in `iperf_result.json` on the
device. Use a unique `--output` path when retaining multiple local runs.

## Read recent results

```powershell
$headers = @{ Authorization = "Bearer $env:DIAGNOSTICS_API_TOKEN" }
Invoke-RestMethod http://127.0.0.1:8080/results?limit=20 -Headers $headers
```

`GET /results` returns summaries but deliberately excludes the raw iPerf JSON.
The raw result remains available in SQLite for later controlled analysis.
