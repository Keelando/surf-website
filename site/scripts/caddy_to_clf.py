#!/usr/bin/env python3
"""
Convert Caddy JSON access logs to Combined Log Format for GoAccess.
Usage: python3 caddy_to_clf.py < caddy.log > combined.log
"""

import json
import sys
from datetime import datetime, timezone
from urllib.parse import quote

def json_to_clf(json_line):
    """Convert a Caddy JSON log entry to Combined Log Format."""
    try:
        data = json.loads(json_line)

        # Only process HTTP access log entries
        if data.get('logger') != 'http.log.access.log0':
            return None

        request = data.get('request', {})
        headers = request.get('headers', {})

        # Use real visitor IP from Cloudflare header (remote_ip is always 127.0.0.1 behind CF)
        cf_ip = headers.get('Cf-Connecting-Ip', [None])[0]
        x_fwd = headers.get('X-Forwarded-For', [None])[0]
        remote_ip = cf_ip or x_fwd or request.get('remote_ip', '-')

        timestamp = data.get('ts', 0)
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        time_str = dt.strftime('%d/%b/%Y:%H:%M:%S +0000')

        method = request.get('method', '-')
        uri = request.get('uri', '-')
        proto = request.get('proto', '-')
        status = data.get('status', 0)
        size = data.get('size', 0)
        user_agent = headers.get('User-Agent', ['-'])[0] if headers.get('User-Agent') else '-'
        referer = headers.get('Referer', ['-'])[0] if headers.get('Referer') else '-'

        # Combined Log Format: IP - - [TIME] "METHOD URI PROTO" STATUS SIZE "REFERER" "USER-AGENT"
        clf_line = f'{remote_ip} - - [{time_str}] "{method} {uri} {proto}" {status} {size} "{referer}" "{user_agent}"'

        return clf_line

    except (json.JSONDecodeError, KeyError, ValueError) as e:
        # Skip malformed lines
        return None

if __name__ == '__main__':
    for line in sys.stdin:
        clf_line = json_to_clf(line.strip())
        if clf_line:
            print(clf_line)
