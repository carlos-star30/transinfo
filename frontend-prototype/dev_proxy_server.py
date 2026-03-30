from __future__ import annotations

import http.server
import mimetypes
import os
import ssl
import socketserver
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


FRONTEND_DIR = Path(__file__).resolve().parent
WORKSPACE_DIR = FRONTEND_DIR.parent
BACKEND_BASE = os.environ.get("DATAFLOW_BACKEND_BASE", "http://127.0.0.1:8000").rstrip("/")
HOST = os.environ.get("DATAFLOW_DEV_HOST", "127.0.0.1").strip() or "127.0.0.1"
PORT = int(os.environ.get("DATAFLOW_DEV_PORT", "8088"))
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
                'window.__DATAFLOW_APP_VERSION__ = "1.0.0";</script>'
            )
            html = html.replace(
                '<script src="./runtime-config.js?v=20260319-v1.6.20"></script>',
                runtime_override,
            )
            html = html.replace(
                '<script src="./runtime-config.js?v=20260314-mapfix18"></script>',
                runtime_override,
            )
            html = html.replace("./app.js?v=20260319-v1.6.20", f"./app.js")
            html = html.replace("./styles.css?v=20260319-v1.6.20", f"./styles.css")
            html = html.replace("./mock-data.js?v=20260319-v1.6.20", f"./mock-data.js")
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
                'window.__DATAFLOW_APP_VERSION__ = "1.0.0";\n'
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

    def _proxy_request(self):
        target_url = f"{BACKEND_BASE}{self.path}"
        body = None
        content_length = int(self.headers.get("Content-Length", "0") or "0")
        if content_length > 0:
            body = self.rfile.read(content_length)

        req = urllib.request.Request(target_url, data=body, method=self.command)

        for key, value in self.headers.items():
            lower = key.lower()
            if lower in {"host", "origin", "referer", "content-length", "connection"}:
                continue
            req.add_header(key, value)

        req.add_header("Origin", BACKEND_BASE)

        def open_backend(request: urllib.request.Request):
            try:
                return urllib.request.urlopen(request, timeout=60)
            except urllib.error.URLError as exc:
                reason = getattr(exc, "reason", None)
                if isinstance(reason, ssl.SSLCertVerificationError):
                    # Local macOS Python may miss CA chain; fallback for dev proxy only.
                    insecure_ctx = ssl._create_unverified_context()
                    return urllib.request.urlopen(request, timeout=60, context=insecure_ctx)
                raise

        try:
            with open_backend(req) as resp:
                payload = resp.read()
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
        httpd.serve_forever()