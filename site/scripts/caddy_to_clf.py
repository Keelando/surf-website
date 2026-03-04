#!/usr/bin/env python3
"""
Convert Caddy JSON access logs to Combined Log Format for GoAccess.
Usage: python3 caddy_to_clf.py < caddy.log > combined.log
"""

import json
import sys
from datetime import datetime
from urllib.parse import quote

def json_to_clf(json_line):
    """Convert a Caddy JSON log entry to Combined Log Format."""
    try:
        data = json.loads(json_line)

        # Only process HTTP access log entries
        if data.get('logger') != 'http.log.access.log0':
            return None

        request = data.get('request', {})

        # Extract fields
        remote_ip = request.get('remote_ip', '-')
        # Convert IPv6 localhost to IPv4
        if remote_ip == '::1':
            remote_ip = '127.0.0.1'

        timestamp = data.get('ts', 0)
        dt = datetime.fromtimestamp(timestamp)
        time_str = dt.strftime('%d/%b/%Y:%H:%M:%S %z') or dt.strftime('%d/%b/%Y:%H:%M:%S +0000')

        method = request.get('method', '-')
        uri = request.get('uri', '-')
        proto = request.get('proto', '-')
        status = data.get('status', 0)
        size = data.get('size', 0)

        # Get user agent and referer from headers
        headers = request.get('headers', {})
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
