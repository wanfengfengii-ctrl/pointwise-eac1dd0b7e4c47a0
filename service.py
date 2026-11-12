import argparse
import json
import os
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit
import ledger

RELEASES = {'r1': 1, 'r2': 2}

def serve(db, release, host, port):
    version = RELEASES[release]
    Path(db).parent.mkdir(parents=True, exist_ok=True)
    c = ledger.connect(db)
    ledger.initialize(c, version)
    if ledger.schema(c) != version:
        c.close()
        raise ledger.LedgerError('FORMAT_MISMATCH')
    c.close()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.0'

        def log_message(self, *args):
            pass

        def send_json(self, status, body):
            data = ledger.canonical(body).encode('utf-8')
            self.send_response(status)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(data)))
            self.send_header('X-Release', release)
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            path = urlsplit(self.path).path
            c = ledger.connect(db)
            stream = None
            try:
                if path == '/health':
                    actual = ledger.schema(c)
                    self.send_json(200 if actual == version else 503,
                                   {'release': release, 'format': actual})
                elif path == '/snapshot':
                    self.send_json(200, ledger.snapshot(c, version))
                elif path == '/export':
                    stream = ledger.export(c, version)
                    first = next(stream, None)
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/x-ndjson; charset=utf-8')
                    self.send_header('X-Release', release)
                    self.end_headers()
                    if first is not None:
                        self.wfile.write((ledger.canonical(first) + '\n').encode('utf-8'))
                        self.wfile.flush()
                    for row in stream:
                        self.wfile.write((ledger.canonical(row) + '\n').encode('utf-8'))
                        self.wfile.flush()
                else:
                    self.send_json(404, {'error': 'NOT_FOUND'})
            except (ledger.LedgerError, sqlite3.Error) as exc:
                if path != '/export':
                    self.send_json(409, {'error': str(exc)})
                else:
                    self.close_connection = True
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                if stream is not None:
                    stream.close()
                c.close()

        def do_POST(self):
            if urlsplit(self.path).path != '/batches':
                self.send_json(404, {'error': 'NOT_FOUND'})
                return
            c = None
            try:
                size = int(self.headers.get('Content-Length', '-1'))
                if size < 0 or size > 8388608:
                    raise ValueError('BAD_LENGTH')
                batch = json.loads(self.rfile.read(size).decode('utf-8'),
                                   parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
                c = ledger.connect(db)
                self.send_json(200, ledger.ingest(c, version, batch))
            except (ValueError, UnicodeError, ledger.LedgerError, sqlite3.Error) as exc:
                self.send_json(409, {'error': str(exc)})
            finally:
                if c is not None:
                    c.close()

    class Server(ThreadingHTTPServer):
        # An ordinary graceful shutdown waits for accepted requests, including slow exports.
        daemon_threads = False
        block_on_close = True

    with Server((host, port), Handler) as server:
        server.serve_forever()

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--db', required=True)
    p.add_argument('--release', choices=sorted(RELEASES), required=True)
    p.add_argument('--host', default=os.environ.get('POINTWISE_HOST', '127.0.0.1'))
    p.add_argument('--port', type=int, default=int(os.environ.get('POINTWISE_PORT', '8765')))
    a = p.parse_args()
    if a.host != '127.0.0.1':
        p.error('host must be 127.0.0.1')
    serve(a.db, a.release, a.host, a.port)
