"""Fake Alluxio S3 Gateway that 307-redirects to a configured worker URL.

Listens on PORT_GATEWAY. On any GET with Range: bytes=0-0, responds with
HTTP 307 + Location header pointing to WORKER_URL + same path. On anything
else (including non-Range requests), also returns 307 — the plugin only
probes with Range 0-0 but this keeps the stub simple.

This mimics Alluxio's multi-worker redirect behavior: Gateway knows the
owner worker for each file and tells the client to go there directly.
"""
from __future__ import annotations

import argparse
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer


class RedirectHandler(BaseHTTPRequestHandler):
    worker_url: str = ""  # class var, set below

    def do_GET(self):
        location = f"{self.worker_url}{self.path}"
        self.send_response(307)
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        self.end_headers()
        sys.stderr.write(f"[fake_gateway] 307 {self.path} -> {location}\n")
        sys.stderr.flush()

    def log_message(self, *a, **kw):
        pass  # silence default logging; we log manually above


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--worker", required=True, help="e.g. http://localhost:29999")
    args = ap.parse_args()

    RedirectHandler.worker_url = args.worker.rstrip("/")
    httpd = HTTPServer(("0.0.0.0", args.port), RedirectHandler)
    sys.stderr.write(
        f"[fake_gateway] listening on :{args.port}, redirecting -> {args.worker}\n"
    )
    sys.stderr.flush()
    httpd.serve_forever()


if __name__ == "__main__":
    main()
