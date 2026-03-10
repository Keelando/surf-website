"""Minimal HTTP server that mirrors production CSP headers for Playwright tests."""

import http.server
import sys
from pathlib import Path

CSP = (
    "default-src 'self'; "
    "script-src 'self' https://unpkg.com https://cdnjs.cloudflare.com "
    "https://cdn.jsdelivr.net https://static.cloudflareinsights.com "
    "'sha256-qdj8Uq9tuzVi4CMZz0ZHXtVRuRwzhKNNctUioI49ZJ4='; "
    "style-src 'self' 'unsafe-inline' https://unpkg.com; "
    "img-src 'self' data: https:; "
    "font-src 'self'; "
    "connect-src 'self'; "
    "frame-ancestors 'none'"
)


class CSPHandler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        if self.path.endswith(".html") or self.path == "/" or "." not in self.path.split("/")[-1]:
            self.send_header("Content-Security-Policy", CSP)
        super().end_headers()


if __name__ == "__main__":
    site_dir = Path(__file__).resolve().parent.parent.parent / "site"
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 4173
    server = http.server.HTTPServer(("127.0.0.1", port), CSPHandler)
    CSPHandler.extensions_map[".js"] = "application/javascript"
    import os
    os.chdir(site_dir)
    print(f"Serving {site_dir} on http://127.0.0.1:{port} with CSP headers")
    server.serve_forever()
