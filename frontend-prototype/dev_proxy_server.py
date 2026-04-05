from __future__ import annotations

import datetime
import http.server
import importlib
import json
import mimetypes
import os
import re
import ssl
import socketserver
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


FRONTEND_DIR = Path(__file__).resolve().parent
WORKSPACE_DIR = FRONTEND_DIR.parent
if str(WORKSPACE_DIR) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_DIR))
BACKEND_BASE = os.environ.get("DATAFLOW_BACKEND_BASE", "http://127.0.0.1:8000").rstrip("/")
HOST = os.environ.get("DATAFLOW_DEV_HOST", "127.0.0.1").strip() or "127.0.0.1"
PORT = int(os.environ.get("DATAFLOW_DEV_PORT", "8088"))
BACKEND_TIMEOUT_SEC = int(os.environ.get("DATAFLOW_BACKEND_TIMEOUT_SEC", "1200"))
STATIC_CACHEABLE_SUFFIXES = {
    ".css",
    ".js",
    ".mjs",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".svg",
    ".woff",
    ".woff2",
    ".ttf",
    ".ico",
    ".xlsx",
}


class ProxyHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(FRONTEND_DIR), **kwargs)

    def do_OPTIONS(self):
        if self.path.startswith("/api/"):
            self._proxy_request()
            return
        super().do_OPTIONS()

    def do_GET(self):
        if self.path == "/" or self.path == "/index.html" or self.path.startswith("/index.html?"):
            index_path = FRONTEND_DIR / "index.html"
            html = index_path.read_text(encoding="utf-8")

            runtime_override = (
                '<script>window.__DATAFLOW_API_BASE__ = window.location.origin; '
                'window.__DATAFLOW_APP_TITLE__ = "转换映射查询"; '
                'window.__DATAFLOW_APP_VERSION__ = "2.0.1";</script>'
            )
            html = re.sub(r'<script\s+src="\./runtime-config\.js(?:\?[^\"]*)?"></script>', runtime_override, html)
            html = re.sub(r'\.\/app\.js\?[^\"\']+', "./app.js", html)
            html = re.sub(r'\.\/styles\.css\?[^\"\']+', "./styles.css", html)
            html = re.sub(r'\.\/mock-data\.js\?[^\"\']+', "./mock-data.js", html)
            body = html.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path.startswith("/reusable/"):
            if self._serve_workspace_static(self.path):
                return
            self.send_error(404, "File not found")
            return

        if self.path == "/runtime-config.js" or self.path.startswith("/runtime-config.js?"):
            body = (
                'window.__DATAFLOW_API_BASE__ = window.location.origin;\n'
                'window.__DATAFLOW_APP_TITLE__ = "转换映射查询";\n'
                'window.__DATAFLOW_APP_VERSION__ = "2.0.1";\n'
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/javascript; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path.startswith("/api/"):
            self._proxy_request()
            return

        super().do_GET()

    def do_POST(self):
        if self.path.startswith("/api/"):
            self._proxy_request()
            return
        self.send_error(405, "Method not allowed")

    def do_PUT(self):
        if self.path.startswith("/api/"):
            self._proxy_request()
            return
        self.send_error(405, "Method not allowed")

    def do_PATCH(self):
        if self.path.startswith("/api/"):
            self._proxy_request()
            return
        self.send_error(405, "Method not allowed")

    def do_DELETE(self):
        if self.path.startswith("/api/"):
            self._proxy_request()
            return
        self.send_error(405, "Method not allowed")

    def end_headers(self):
        parsed = urllib.parse.urlparse(self.path)
        request_path = parsed.path or ""
        suffix = Path(request_path).suffix.lower()

        if request_path.startswith("/api/") or request_path in {"/", "/index.html", "/runtime-config.js"}:
            cache_control = "no-store"
        elif suffix in {".css", ".js", ".mjs", ".html"}:
            cache_control = "no-store"
        elif suffix in STATIC_CACHEABLE_SUFFIXES:
            cache_control = "public, max-age=31536000, immutable" if parsed.query else "public, max-age=3600"
        else:
            cache_control = "no-store"

        self.send_header("Cache-Control", cache_control)
        super().end_headers()

    def _serve_workspace_static(self, request_path: str) -> bool:
        parsed_path = urllib.parse.urlparse(request_path).path
        relative_path = parsed_path.lstrip("/")
        target_path = (WORKSPACE_DIR / relative_path).resolve()
        try:
            target_path.relative_to(WORKSPACE_DIR)
        except ValueError:
            return False

        if not target_path.is_file():
            return False

        payload = target_path.read_bytes()
        content_type, _ = mimetypes.guess_type(str(target_path))
        self.send_response(200)
        self.send_header("Content-Type", content_type or "application/octet-stream")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)
        return True

    def _handle_local_mapping_patch_api(self, body: bytes | None) -> bool:
        parsed = urllib.parse.urlparse(self.path)
        if self.command != "POST" or parsed.path not in {"/api/path-selection/mapping", "/api/path-selection/text", "/api/path-selection/logic"}:
            return False

        try:
            payload = json.loads((body or b"{}").decode("utf-8") or "{}")
        except json.JSONDecodeError as exc:
            error_payload = json.dumps({"detail": f"Invalid JSON: {exc.msg}"}, ensure_ascii=False).encode("utf-8")
            self.send_response(400)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", f"http://localhost:{PORT}")
            self.send_header("Access-Control-Allow-Credentials", "true")
            self.send_header("Content-Length", str(len(error_payload)))
            self.end_headers()
            self.wfile.write(error_payload)
            return True

        os.environ.setdefault("DB_DRIVER", "sqlite")
        os.environ.setdefault("SQLITE_DB_PATH", str(WORKSPACE_DIR / "backend" / "data" / "trans_fields_mapping.db"))

        try:
            backend_module = importlib.import_module("backend.import_status_api")
            backend_module = importlib.reload(backend_module)

            PathSegmentRequest = backend_module.PathSegmentRequest
            build_path_logic_payload = backend_module.build_path_logic_payload
            build_path_mapping_payload = backend_module.build_path_mapping_payload
            build_path_text_payload = backend_module.build_path_text_payload

            raw_segments = payload.get("segments") or []
            segments = [PathSegmentRequest(**segment) for segment in raw_segments if isinstance(segment, dict)]
            if parsed.path.endswith("/logic"):
                response_obj = build_path_logic_payload(segments)
            elif parsed.path.endswith("/text"):
                response_obj = build_path_text_payload(segments)
            else:
                response_obj = build_path_mapping_payload(
                    segments,
                    include_logic=bool(payload.get("include_logic")),
                    include_text=bool(payload.get("include_text")),
                )
            response_payload = json.dumps(response_obj, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", f"http://localhost:{PORT}")
            self.send_header("Access-Control-Allow-Credentials", "true")
            self.send_header("Content-Length", str(len(response_payload)))
            self.end_headers()
            self.wfile.write(response_payload)
            return True
        except Exception as exc:  # pragma: no cover - local helper path
            error_payload = json.dumps({"detail": str(exc)}, ensure_ascii=False).encode("utf-8")
            self.send_response(502)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", f"http://localhost:{PORT}")
            self.send_header("Access-Control-Allow-Credentials", "true")
            self.send_header("Content-Length", str(len(error_payload)))
            self.end_headers()
            self.wfile.write(error_payload)
            return True

    def _proxy_request(self):
        start_time = time.time()
        timestamp = datetime.datetime.now().strftime('%H:%M:%S.%f')[:-3]
        
        body = None
        content_length = int(self.headers.get("Content-Length", "0") or "0")
        if content_length > 0:
            body = self.rfile.read(content_length)

        if self._handle_local_mapping_patch_api(body):
            return

        target_url = f"{BACKEND_BASE}{self.path}"

        req = urllib.request.Request(target_url, data=body, method=self.command)

        for key, value in self.headers.items():
            lower = key.lower()
            if lower in {"host", "origin", "referer", "content-length", "connection"}:
                continue
            req.add_header(key, value)

        req.add_header("Origin", BACKEND_BASE)

        def open_backend(request: urllib.request.Request):
            try:
                return urllib.request.urlopen(request, timeout=BACKEND_TIMEOUT_SEC)
            except urllib.error.URLError as exc:
                reason = getattr(exc, "reason", None)
                if isinstance(reason, ssl.SSLCertVerificationError):
                    # Local macOS Python may miss CA chain; fallback for dev proxy only.
                    insecure_ctx = ssl._create_unverified_context()
                    return urllib.request.urlopen(request, timeout=BACKEND_TIMEOUT_SEC, context=insecure_ctx)
                raise

        try:
            with open_backend(req) as resp:
                payload = resp.read()
                elapsed = time.time() - start_time
                print(f"[PROXY PERF] {timestamp} {self.command} {self.path} -> {resp.status} ({elapsed*1000:.1f}ms)")
                self.send_response(resp.status)
                for key, value in resp.headers.items():
                    lower = key.lower()
                    if lower in {"content-length", "transfer-encoding", "connection", "content-encoding", "access-control-allow-origin", "access-control-allow-credentials", "set-cookie"}:
                        continue
                    self.send_header(key, value)

                cookies = resp.headers.get_all("Set-Cookie") or []
                for cookie in cookies:
                    self.send_header("Set-Cookie", self._rewrite_cookie(cookie))

                self.send_header("Access-Control-Allow-Origin", f"http://localhost:{PORT}")
                self.send_header("Access-Control-Allow-Credentials", "true")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
        except urllib.error.HTTPError as exc:
            payload = exc.read()
            elapsed = time.time() - start_time
            print(f"[PROXY PERF] {timestamp} {self.command} {self.path} -> {exc.code} ({elapsed*1000:.1f}ms)")
            self.send_response(exc.code)
            for key, value in exc.headers.items():
                lower = key.lower()
                if lower in {"content-length", "transfer-encoding", "connection", "content-encoding", "access-control-allow-origin", "access-control-allow-credentials", "set-cookie"}:
                    continue
                self.send_header(key, value)
            cookies = exc.headers.get_all("Set-Cookie") or []
            for cookie in cookies:
                self.send_header("Set-Cookie", self._rewrite_cookie(cookie))
            self.send_header("Access-Control-Allow-Origin", f"http://localhost:{PORT}")
            self.send_header("Access-Control-Allow-Credentials", "true")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except Exception as exc:  # pragma: no cover - local helper path
            payload = str(exc).encode("utf-8")
            elapsed = time.time() - start_time
            print(f"[PROXY PERF] {timestamp} {self.command} {self.path} -> ERROR ({elapsed*1000:.1f}ms): {exc}")
            self.send_response(502)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", f"http://localhost:{PORT}")
            self.send_header("Access-Control-Allow-Credentials", "true")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    @staticmethod
    def _rewrite_cookie(cookie: str) -> str:
        updated = cookie.replace("; Secure", "")
        updated = updated.replace("; SameSite=none", "; SameSite=Lax")
        updated = updated.replace("; SameSite=None", "; SameSite=Lax")
        return updated


class ThreadingTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True


if __name__ == "__main__":
    with ThreadingTCPServer((HOST, PORT), ProxyHandler) as httpd:
        print(f"Serving local frontend proxy at http://{HOST}:{PORT}")
        print(f"Proxying /api to {BACKEND_BASE}")
        print(f"Backend request timeout: {BACKEND_TIMEOUT_SEC}s")
        httpd.serve_forever()