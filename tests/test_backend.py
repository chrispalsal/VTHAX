import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib import error, request

from backend.server import DiagnosticHandler
from backend.storage import ResultStore


SAMPLE_RECORD = {
    "measured_at": "2026-09-19T17:00:00+00:00",
    "participant_id": "participant-001",
    "server": "10.0.0.10",
    "port": 5201,
    "direction": "upload",
    "protocol": "TCP",
    "summary": {
        "sent_mbps": 42.5,
        "received_mbps": 41.9,
        "retransmits": 2,
    },
    "iperf_result": {"start": {}, "end": {}},
}

SAMPLE_TELEMETRY = {
    "measured_at": "2026-09-19T17:01:00+00:00",
    "session_id": "session-001",
    "participant_id": "participant-001",
    "consent": {"location": True, "connectivity": True},
    "location": {
        "available": True,
        "latitude": 38.03,
        "longitude": -78.51,
        "accuracy_m": 25,
    },
    "connectivity": {
        "online": True,
        "effective_type": "4g",
        "downlink_mbps": 8.5,
    },
}


class ResultStoreTests(unittest.TestCase):
    def test_insert_and_list(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ResultStore(Path(directory) / "diagnostics.sqlite3")
            result_id = store.insert(SAMPLE_RECORD)

            results = store.list()

        self.assertEqual(result_id, 1)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["participant_id"], "participant-001")
        self.assertEqual(results[0]["summary"]["sent_mbps"], 42.5)
        self.assertNotIn("iperf_result", results[0])

    def test_invalid_protocol_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ResultStore(Path(directory) / "diagnostics.sqlite3")
            record = {**SAMPLE_RECORD, "protocol": "ICMP"}
            with self.assertRaisesRegex(ValueError, "protocol"):
                store.insert(record)

    def test_insert_and_list_telemetry(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ResultStore(Path(directory) / "diagnostics.sqlite3")
            sample_id = store.insert_telemetry(SAMPLE_TELEMETRY)
            samples = store.list_telemetry()

        self.assertEqual(sample_id, 1)
        self.assertEqual(samples[0]["session_id"], "session-001")
        self.assertTrue(samples[0]["consent"]["location"])
        self.assertEqual(samples[0]["connectivity"]["effective_type"], "4g")

    def test_telemetry_without_consent_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ResultStore(Path(directory) / "diagnostics.sqlite3")
            sample = {
                **SAMPLE_TELEMETRY,
                "consent": {"location": False, "connectivity": False},
            }
            with self.assertRaisesRegex(ValueError, "permission"):
                store.insert_telemetry(sample)


class CollectorHttpTests(unittest.TestCase):
    def setUp(self):
        self.temp_directory = tempfile.TemporaryDirectory()
        DiagnosticHandler.store = ResultStore(
            Path(self.temp_directory.name) / "diagnostics.sqlite3"
        )
        DiagnosticHandler.api_token = "test-token"
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), DiagnosticHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        host, port = self.server.server_address
        self.base_url = f"http://{host}:{port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.temp_directory.cleanup()

    def _request(self, method, path, payload=None, token="test-token"):
        headers = {}
        data = None
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if payload is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(payload).encode("utf-8")
        http_request = request.Request(
            self.base_url + path,
            data=data,
            headers=headers,
            method=method,
        )
        with request.urlopen(http_request, timeout=2) as response:
            return response.status, json.loads(response.read())

    def test_post_then_get_result(self):
        status, created = self._request("POST", "/results", SAMPLE_RECORD)
        self.assertEqual(status, 201)
        self.assertEqual(created, {"id": 1})

        status, body = self._request("GET", "/results?limit=10")
        self.assertEqual(status, 200)
        self.assertEqual(len(body["results"]), 1)
        self.assertEqual(body["results"][0]["id"], 1)

    def test_missing_token_is_rejected(self):
        with self.assertRaises(error.HTTPError) as caught:
            self._request("GET", "/results", token=None)
        self.assertEqual(caught.exception.code, 401)

    def test_post_then_get_telemetry(self):
        status, created = self._request("POST", "/telemetry", SAMPLE_TELEMETRY)
        self.assertEqual(status, 201)
        self.assertEqual(created, {"id": 1})

        status, body = self._request("GET", "/telemetry?limit=10")
        self.assertEqual(status, 200)
        self.assertEqual(body["samples"][0]["session_id"], "session-001")


if __name__ == "__main__":
    unittest.main()
