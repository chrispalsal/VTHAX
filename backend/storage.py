from __future__ import annotations

import datetime
import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any


SCHEMA = """
CREATE TABLE IF NOT EXISTS diagnostic_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    received_at TEXT NOT NULL,
    measured_at TEXT NOT NULL,
    participant_id TEXT,
    server TEXT NOT NULL,
    port INTEGER NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('upload', 'download')),
    protocol TEXT NOT NULL CHECK (protocol IN ('TCP', 'UDP')),
    summary_json TEXT NOT NULL,
    iperf_result_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_results_measured_at
ON diagnostic_results(measured_at DESC);

CREATE INDEX IF NOT EXISTS idx_results_participant_id
ON diagnostic_results(participant_id);

CREATE TABLE IF NOT EXISTS telemetry_samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    received_at TEXT NOT NULL,
    measured_at TEXT NOT NULL,
    session_id TEXT NOT NULL,
    participant_id TEXT,
    consent_location INTEGER NOT NULL,
    consent_connectivity INTEGER NOT NULL,
    location_json TEXT,
    connectivity_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_telemetry_measured_at
ON telemetry_samples(measured_at DESC);

CREATE INDEX IF NOT EXISTS idx_telemetry_session_id
ON telemetry_samples(session_id);
"""


def utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


class ResultStore:
    def __init__(self, database_path: str | Path):
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as connection:
            with connection:
                connection.executescript(SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        return connection

    def insert(self, record: dict[str, Any]) -> int:
        normalized = validate_record(record)
        with closing(self._connect()) as connection:
            with connection:
                cursor = connection.execute(
                    """
                    INSERT INTO diagnostic_results (
                        received_at, measured_at, participant_id, server, port,
                        direction, protocol, summary_json, iperf_result_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        utc_now(),
                        normalized["measured_at"],
                        normalized.get("participant_id"),
                        normalized["server"],
                        normalized["port"],
                        normalized["direction"],
                        normalized["protocol"],
                        json.dumps(normalized["summary"], separators=(",", ":")),
                        json.dumps(normalized["iperf_result"], separators=(",", ":")),
                    ),
                )
                result_id = int(cursor.lastrowid)
        return result_id

    def list(self, limit: int = 100) -> list[dict[str, Any]]:
        limit = max(1, min(limit, 1000))
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT id, received_at, measured_at, participant_id, server,
                       port, direction, protocol, summary_json
                FROM diagnostic_results
                ORDER BY measured_at DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        return [
            {
                "id": row["id"],
                "received_at": row["received_at"],
                "measured_at": row["measured_at"],
                "participant_id": row["participant_id"],
                "server": row["server"],
                "port": row["port"],
                "direction": row["direction"],
                "protocol": row["protocol"],
                "summary": json.loads(row["summary_json"]),
            }
            for row in rows
        ]

    def insert_telemetry(self, sample: dict[str, Any]) -> int:
        normalized = validate_telemetry(sample)
        with closing(self._connect()) as connection:
            with connection:
                cursor = connection.execute(
                    """
                    INSERT INTO telemetry_samples (
                        received_at, measured_at, session_id, participant_id,
                        consent_location, consent_connectivity,
                        location_json, connectivity_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        utc_now(),
                        normalized["measured_at"],
                        normalized["session_id"],
                        normalized.get("participant_id"),
                        int(normalized["consent"]["location"]),
                        int(normalized["consent"]["connectivity"]),
                        _json_or_none(normalized.get("location")),
                        _json_or_none(normalized.get("connectivity")),
                    ),
                )
                sample_id = int(cursor.lastrowid)
        return sample_id

    def list_telemetry(self, limit: int = 100) -> list[dict[str, Any]]:
        limit = max(1, min(limit, 1000))
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT id, received_at, measured_at, session_id, participant_id,
                       consent_location, consent_connectivity,
                       location_json, connectivity_json
                FROM telemetry_samples
                ORDER BY measured_at DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            {
                "id": row["id"],
                "received_at": row["received_at"],
                "measured_at": row["measured_at"],
                "session_id": row["session_id"],
                "participant_id": row["participant_id"],
                "consent": {
                    "location": bool(row["consent_location"]),
                    "connectivity": bool(row["consent_connectivity"]),
                },
                "location": _parse_json_or_none(row["location_json"]),
                "connectivity": _parse_json_or_none(row["connectivity_json"]),
            }
            for row in rows
        ]


def validate_record(record: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(record, dict):
        raise ValueError("request body must be a JSON object")

    required = (
        "measured_at",
        "server",
        "port",
        "direction",
        "protocol",
        "summary",
        "iperf_result",
    )
    missing = [field for field in required if field not in record]
    if missing:
        raise ValueError(f"missing required fields: {', '.join(missing)}")

    try:
        datetime.datetime.fromisoformat(str(record["measured_at"]).replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("measured_at must be an ISO-8601 timestamp") from error

    normalized = dict(record)
    normalized["server"] = str(record["server"])[:255]
    normalized["port"] = int(record["port"])
    normalized["direction"] = str(record["direction"]).lower()
    normalized["protocol"] = str(record["protocol"]).upper()

    if not 1 <= normalized["port"] <= 65535:
        raise ValueError("port must be between 1 and 65535")
    if normalized["direction"] not in {"upload", "download"}:
        raise ValueError("direction must be upload or download")
    if normalized["protocol"] not in {"TCP", "UDP"}:
        raise ValueError("protocol must be TCP or UDP")
    if not isinstance(record["summary"], dict):
        raise ValueError("summary must be a JSON object")
    if not isinstance(record["iperf_result"], dict):
        raise ValueError("iperf_result must be a JSON object")

    participant_id = record.get("participant_id")
    normalized["participant_id"] = (
        str(participant_id)[:128] if participant_id is not None else None
    )
    return normalized


def validate_telemetry(sample: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(sample, dict):
        raise ValueError("request body must be a JSON object")
    required = ("measured_at", "session_id", "consent")
    missing = [field for field in required if field not in sample]
    if missing:
        raise ValueError(f"missing required fields: {', '.join(missing)}")
    try:
        datetime.datetime.fromisoformat(str(sample["measured_at"]).replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("measured_at must be an ISO-8601 timestamp") from error
    if not isinstance(sample["consent"], dict):
        raise ValueError("consent must be a JSON object")

    consent = {
        "location": sample["consent"].get("location") is True,
        "connectivity": sample["consent"].get("connectivity") is True,
    }
    if not consent["location"] and not consent["connectivity"]:
        raise ValueError("at least one collection permission is required")

    normalized = dict(sample)
    normalized["session_id"] = str(sample["session_id"])[:128]
    if not normalized["session_id"]:
        raise ValueError("session_id cannot be empty")
    participant_id = sample.get("participant_id")
    normalized["participant_id"] = (
        str(participant_id)[:128] if participant_id is not None else None
    )
    normalized["consent"] = consent

    for field in ("location", "connectivity"):
        value = sample.get(field)
        if value is not None and not isinstance(value, dict):
            raise ValueError(f"{field} must be a JSON object or null")
        if value is not None and not consent[field]:
            raise ValueError(f"{field} data was supplied without consent")
    return normalized


def _json_or_none(value: Any) -> str | None:
    return json.dumps(value, separators=(",", ":")) if value is not None else None


def _parse_json_or_none(value: str | None) -> Any:
    return json.loads(value) if value is not None else None
