"""Generation worker process.

A worker owns one committed generation.  It never migrates the database and
never decides that it is isolated from a PID file or from the port being free;
it simply compares the durable committed generation with the generation it was
started for on every acceptance decision.  The supervisor hands it an already
bound listening socket and a control pipe.  Closing that socket stops new
connections (so the next generation can take over with SO_REUSEPORT) while
already accepted export threads keep draining their pinned snapshots.  EOF on
the control pipe has the same meaning when the supervisor disappears.
"""

import argparse
import os
import signal
import socket
import sys
import threading
import traceback

import ledger
from . import service

RELEASES = {'r1': 1, 'r2': 2}


def main(argv=None):
    parser = argparse.ArgumentParser(prog='pointwise-worker')
    parser.add_argument('--db', required=True)
    parser.add_argument('--release', choices=sorted(RELEASES), required=True)
    parser.add_argument('--format', type=int, required=True)
    parser.add_argument('--generation', type=int, required=True)
    parser.add_argument('--state', required=True)
    parser.add_argument('--host', required=True)
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--listen-fd', type=int, required=True)
    parser.add_argument('--control-fd', type=int, default=None)
    args = parser.parse_args(argv)

    version = RELEASES[args.release]
    if args.format != version:
        raise SystemExit('worker release/format mismatch')

    listener = socket.socket(fileno=args.listen_fd)
    listener.setblocking(True)
    server = service.LedgerServer(
        listener, (args.host, args.port), service.Handler)
    server.db = args.db
    server.state_dir = args.state
    server.release = args.release
    server.version = version
    server.generation = args.generation
    server.write_lock = os.path.join(args.state, 'write.lock')

    started = threading.Event()

    def run_server():
        started.set()
        try:
            server.serve_forever(0.05)
        except BaseException:
            traceback.print_exc()

    thread = threading.Thread(target=run_server, name='http-accept')
    thread.start()
    started.wait()

    closed = threading.Event()
    close_lock = threading.Lock()

    def close_listener():
        with close_lock:
            if closed.is_set():
                return
            closed.set()
        try:
            server.shutdown()
        except BaseException:
            traceback.print_exc()

    stop = threading.Event()

    def on_signal(signum, frame):
        stop.set()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    control = args.control_fd
    try:
        if control is None:
            stop.wait()
        else:
            while not stop.is_set():
                try:
                    data = os.read(control, 4096)
                except OSError:
                    break
                if not data:
                    break
                for token in data.decode('utf-8', 'replace').split():
                    if token in ('close-listen', 'stop'):
                        close_listener()
                        stop.set()
    finally:
        close_listener()
        # Closing the inherited listener here is the handover point.  Then
        # join request threads so live exports finish against their snapshot.
        try:
            server.server_close()
        except BaseException:
            traceback.print_exc()
        thread.join()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
