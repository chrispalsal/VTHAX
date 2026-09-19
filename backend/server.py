import argparse
import hmac
import json
import mimetypes
import os
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

try:
    from .storage import ResultStore
except ImportError:
    from storage import ResultStore


MAX_BODY_BYTES = 10 * 1024 * 1024


class DiagnosticHandler(BaseHTTPRequestHandler):
    store: ResultStore
    api_token: str | None = None
    allowed_origin: str | None = None
    static_directory: Path | None = None

    def _send_cors_headers(self) -> None:
        origin = self.headers.get("Origin")
        if self.allowed_origin and origin == self.allowed_origin:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")

    def _send_json(self, status: HTTPStatus, payload: object) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self._send_cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def _send_static(self, path: str) -> bool:
        if not self.static_directory:
            return False
        relative_path = "index.html" if path == "/" else unquote(path).lstrip("/")
        root = self.static_directory.resolve()
        candidate = (root / relative_path).resolve()
        try:
            candidate.relative_to(root)
        except ValueError:
            return False
        if not candidate.is_file():
            return False

        body = candidate.read_bytes()
        content_type = mimetypes.guess_type(candidate.name)[0] or "application/octet-stream"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(body)
        return True

    def _authorized(self) -> bool:
        if not self.api_token:
            return True
        expected = f"Bearer {self.api_token}"
        supplied = self.headers.get("Authorization", "")
        return hmac.compare_digest(supplied, expected)

    def _require_authorization(self) -> bool:
        if self._authorized():
            return True
        self._send_json(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
        return False

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._send_json(HTTPStatus.OK, {"status": "ok"})
            return
        if parsed.path in {"/results", "/telemetry"}:
            if not self._require_authorization():
                return
            try:
                limit = int(parse_qs(parsed.query).get("limit", ["100"])[0])
            except ValueError:
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": "limit must be an integer"})
                return
            payload = (
                {"results": self.store.list(limit)}
                if parsed.path == "/results"
                else {"samples": self.store.list_telemetry(limit)}
            )
            self._send_json(HTTPStatus.OK, payload)
            return
        if self._send_static(parsed.path):
            return
        else:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        if path not in {"/results", "/telemetry"}:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})
            return
        if not self._require_authorization():
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "invalid Content-Length"})
            return
        if length <= 0 or length > MAX_BODY_BYTES:
            self._send_json(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                {"error": f"body must be between 1 and {MAX_BODY_BYTES} bytes"},
            )
            return

        try:
            payload = json.loads(self.rfile.read(length))
            result_id = (
                self.store.insert(payload)
                if path == "/results"
                else self.store.insert_telemetry(payload)
            )
        except (json.JSONDecodeError, UnicodeDecodeError):
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "invalid JSON"})
            return
        except (TypeError, ValueError) as error:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(error)})
            return

        self._send_json(HTTPStatus.CREATED, {"id": result_id})

    def do_OPTIONS(self) -> None:
        origin = self.headers.get("Origin")
        if not self.allowed_origin or origin != self.allowed_origin:
            self._send_json(HTTPStatus.FORBIDDEN, {"error": "origin not allowed"})
            return
        self.send_response(HTTPStatus.NO_CONTENT)
        self._send_cors_headers()
        self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Max-Age", "600")
        self.end_headers()


def main() -> None:
    parser = argparse.ArgumentParser(description="Store iPerf diagnostic results")
    parser.add_argument("--bind", default="172.29.82.246")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--database", default="backend/data/diagnostics.sqlite3")
    parser.add_argument("--static-dir", default="dist")
    args = parser.parse_args()

    api_token = os.environ.get("DIAGNOSTICS_API_TOKEN")
    if (
        args.bind not in {"127.0.0.1", "172.29.82.246", "localhost", "::1"}
        and not api_token
    ):
        parser.error(
            "DIAGNOSTICS_API_TOKEN must be set when listening beyond localhost"
        )
    DiagnosticHandler.store = ResultStore(args.database)
    DiagnosticHandler.api_token = api_token
    DiagnosticHandler.allowed_origin = os.environ.get("DIAGNOSTICS_ALLOWED_ORIGIN")
    DiagnosticHandler.static_directory = Path(args.static_dir)
    server = ThreadingHTTPServer((args.bind, args.port), DiagnosticHandler)
    print(f"Diagnostic collector listening on http://{args.bind}:{args.port}")
    if not DiagnosticHandler.api_token:
        print("Warning: DIAGNOSTICS_API_TOKEN is not set; requests are unauthenticated.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nCollector stopped.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
