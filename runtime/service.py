"""Generation-aware HTTP transport for the pointwise ledger runtime.

The worker process is given an already-bound listening socket, the committed
generation it was started for, and the release/format that generation owns.
Every write first observes the durable deployment state: while a publication
journal exists, or after the committed generation has moved, the worker
refuses writes with a retryable 503 before touching the ledger.  Exports already
streaming keep their already opened SQLite read transaction and are never
truncated by a switch.
"""

import fcntl
import json
import os
import sqlite3
from http.server import BaseHTTPRequestHandler
from socketserver import ThreadingMixIn, TCPServer

import ledger

RETRY_AFTER = '1'


def read_json_file(path):
    try:
        with open(path, 'rb') as handle:
            return json.loads(handle.read().decode('utf-8'))
    except (FileNotFoundError, ValueError, OSError):
        return None


def read_state(state_dir):
    return read_json_file(os.path.join(state_dir, 'state.json'))


def pending_exists(state_dir):
    return os.path.exists(os.path.join(state_dir, 'pending.json'))


def state_gate(state_dir, generation, version):
    """Return a refusal reason for this worker, or None when it may accept.

    Reading the atomically renamed state file and the journal marker is the
    acceptance boundary.  A stale worker can therefore never admit a write
    into a newer generation even if it still owns delivered connections.
    """
    state = read_state(state_dir)
    if state is None:
        return 'UNINSTALLED'
    if pending_exists(state_dir):
        return 'PUBLICATION_IN_PROGRESS'
    if state.get('generation') != generation:
        return 'STALE_GENERATION'
    if state.get('format') != version:
        return 'FORMAT_MISMATCH'
    return None


class FileLock:
    """Advisory process lock used to serialize writes against a migration."""

    def __init__(self, path, exclusive):
        self.path = path
        self.exclusive = exclusive
        self.fd = None

    def __enter__(self):
        self.fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        fcntl.flock(self.fd, fcntl.LOCK_EX if self.exclusive else fcntl.LOCK_SH)
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
        finally:
            os.close(self.fd)
            self.fd = None
        return False


class LedgerServer(ThreadingMixIn, TCPServer):
    """Threading server built around an inherited, already-listening socket."""

    daemon_threads = False
    block_on_close = True

    def __init__(self, listener, address, handler):
        self.address_family = listener.family
        # TCPServer allocates an unused socket when bind_and_activate is false;
        # replace it with the descriptor handed over by the supervisor.
        TCPServer.__init__(self, address, handler, bind_and_activate=False)
        self.socket.close()
        self.socket = listener
        self.server_address = address
        self.server_name = address[0]
        self.server_port = address[1]

    def fileno(self):
        return self.socket.fileno()

    def server_close(self):
        try:
            self.socket.close()
        except OSError:
            pass
        # ThreadingMixIn joins non-daemon request threads (including exports).
        super().server_close()


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.0'

    def log_message(self, *args):
        pass

    def send_json(self, status, body, extra_headers=None):
        data = ledger.canonical(body).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(data)))
        self.send_header('X-Release', self.server.release)
        self.send_header('X-Generation', str(self.server.generation))
        if extra_headers:
            for key, value in extra_headers.items():
                self.send_header(key, value)
        self.end_headers()
        self.wfile.write(data)

    def send_retry(self, reason):
        self.send_json(503, {'error': reason, 'retryable': True},
                       {'Retry-After': RETRY_AFTER})

    def refusal(self):
        return state_gate(self.server.state_dir,
                          self.server.generation,
                          self.server.version)

    def do_GET(self):
        path = self.path.split('?', 1)[0]
        c = None
        stream = None
        try:
            c = ledger.connect(self.server.db)
            if path == '/health':
                try:
                    actual = ledger.schema(c)
                except (ledger.LedgerError, sqlite3.Error):
                    actual = -1
                reason = self.refusal()
                body = {'release': self.server.release, 'format': actual,
                        'generation': self.server.generation}
                if reason is None and actual == self.server.version:
                    self.send_json(200, body)
                else:
                    body['error'] = reason or 'FORMAT_MISMATCH'
                    self.send_json(503, body, {'Retry-After': RETRY_AFTER})
                return
            if path == '/snapshot':
                reason = self.refusal()
                if reason is not None:
                    self.send_retry(reason)
                    return
                self.send_json(200, ledger.snapshot(c, self.server.version))
                return
            if path == '/export':
                reason = self.refusal()
                if reason is not None:
                    self.send_retry(reason)
                    return
                stream = ledger.export(c, self.server.version)
                # Pulling the first row pins the read transaction before the
                # success headers are sent; the rest streams from that same
                # snapshot regardless of later switches.
                first = next(stream, None)
                self.send_response(200)
                self.send_header('Content-Type',
                                 'application/x-ndjson; charset=utf-8')
                self.send_header('X-Release', self.server.release)
                self.send_header('X-Generation', str(self.server.generation))
                self.end_headers()
                if first is not None:
                    self.wfile.write(
                        (ledger.canonical(first) + '\n').encode('utf-8'))
                    self.wfile.flush()
                for row in stream:
                    self.wfile.write(
                        (ledger.canonical(row) + '\n').encode('utf-8'))
                    self.wfile.flush()
                return
            self.send_json(404, {'error': 'NOT_FOUND'})
        except (ledger.LedgerError, sqlite3.Error) as exc:
            if path == '/export':
                self.close_connection = True
            else:
                self.send_json(409, {'error': str(exc)})
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True
        finally:
            if stream is not None:
                stream.close()
            if c is not None:
                c.close()

    def do_POST(self):
        path = self.path.split('?', 1)[0]
        if path != '/batches':
            self.send_json(404, {'error': 'NOT_FOUND'})
            return
        reason = self.refusal()
        if reason is not None:
            self.send_retry(reason)
            return
        c = None
        try:
            size = int(self.headers.get('Content-Length', '-1'))
            if size < 0 or size > 8388608:
                raise ValueError('BAD_LENGTH')
            batch = json.loads(
                self.rfile.read(size).decode('utf-8'),
                parse_constant=lambda value: (_ for _ in ()).throw(
                    ValueError(value)))
            # Serialize against the migration's exclusive lock, then re-read
            # the durable state so a request admitted just before the pause is
            # either completed before migration or refused before ingest.
            with FileLock(self.server.write_lock, False):
                reason = self.refusal()
                if reason is not None:
                    self.send_retry(reason)
                    return
                c = ledger.connect(self.server.db)
                self.send_json(200, ledger.ingest(
                    c, self.server.version, batch))
        except (ValueError, UnicodeError, ledger.LedgerError,
                sqlite3.Error) as exc:
            if str(exc) == 'FORMAT_MISMATCH':
                self.send_retry(str(exc))
            else:
                self.send_json(409, {'error': str(exc)})
        finally:
            if c is not None:
                c.close()
