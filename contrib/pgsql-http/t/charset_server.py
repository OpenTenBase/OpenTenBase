# SPDX-FileCopyrightText: 2026 messere1
# SPDX-License-Identifier: BSD-3-Clause

# Local response fixtures for response_charset.sql; no external API is used.
from http.server import BaseHTTPRequestHandler, HTTPServer

TYPES = {
    '/plain': 'text/plain; charset=ISO-8859-1',
    '/quoted': 'text/plain; charset="ISO-8859-1"',
    '/spaces': 'text/plain; charset = ISO-8859-1 ',
    '/mixed': 'text/plain; ChArSeT="ISO-8859-1"',
    '/other-quoted': 'text/plain; note="a; charset=UTF-8"; charset=ISO-8859-1',
}
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', TYPES[self.path])
        self.send_header('Content-Length', '4')
        self.end_headers()
        self.wfile.write(b'caf\xe9')

HTTPServer(('127.0.0.1', 9081), Handler).serve_forever()
