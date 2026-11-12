#!/usr/bin/env python3
"""Pointwise release manager.

Fixed contract entry points:

    python3 -B ops.py build --release r1 --out ARTIFACT
    python3 -B ops.py promote --state STATE --artifact ARTIFACT --expect N --request-id ID
    python3 -B ops.py rollback --state STATE --expect N --request-id ID
    python3 -B ops.py run --state STATE --host 127.0.0.1 --port PORT

Additional operational entry points: verify, demo, bootstrap, recover, status,
agent-install, agent-uninstall.  The first successful promotion installs a
self-contained copy of this manager at STATE/bin/ops.py and reports its
absolute path.

Publication order is deliberately durable:

    validate immutable package -> pending.json -> exclusive write gate ->
    ledger.migrate() transaction -> state.json (atomic rename) -> remove pending

A coordinator killed anywhere in that sequence leaves either no change or a
journal that the next coordinator completes.  The database conversion itself
is the existing ledger.migrate() transaction; no backup is restored.
"""

import argparse
import errno
import fcntl
import hashlib
import importlib
import io
import json
import os
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import uuid
import zipfile

RELEASES = {'r1': 1, 'r2': 2}
DB_NAME = 'ledger.sqlite'
STATE_FILE = 'state.json'
PENDING_FILE = 'pending.json'
SOURCE_FILES = (
    '__main__.py',
    'ledger.py',
    'samples.py',
    'checks.py',
    'runtime/__init__.py',
    'runtime/service.py',
    'runtime/worker.py',
)


class OpsError(Exception):
    pass


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False)


def _fsync_dir(directory):
    try:
        fd = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def atomic_write(path, data, mode=0o644):
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    temporary = os.path.join(
        directory, '.tmp-%d-%s' % (os.getpid(), uuid.uuid4().hex))
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(fd, 'wb') as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise
    _fsync_dir(directory)


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(65536), b''):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path):
    try:
        with open(path, 'rb') as handle:
            return json.loads(handle.read().decode('utf-8'))
    except (FileNotFoundError, ValueError, OSError):
        return None


def load_state(state_dir):
    return read_json(os.path.join(state_dir, STATE_FILE))


def load_pending(state_dir):
    return read_json(os.path.join(state_dir, PENDING_FILE))


def state_generation(state):
    return 0 if state is None else int(state['generation'])


def db_path(state_dir, state=None):
    name = DB_NAME if state is None else state.get('db', DB_NAME)
    return os.path.join(state_dir, name)


def package_path(state_dir, digest):
    return os.path.join(state_dir, 'packages', digest + '.pyz')


class StateLock:
    def __init__(self, state_dir, blocking=True, name='lock'):
        self.path = os.path.join(state_dir, name)
        self.blocking = blocking
        self.fd = None

    def __enter__(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self.fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        operation = fcntl.LOCK_EX | (0 if self.blocking else fcntl.LOCK_NB)
        try:
            fcntl.flock(self.fd, operation)
        except BlockingIOError:
            os.close(self.fd)
            self.fd = None
            raise
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
        finally:
            os.close(self.fd)
            self.fd = None
        return False


def _activate_artifact(path, validate=True):
    if validate:
        validate_artifact(path)
    if path not in sys.path:
        sys.path.insert(0, path)
    for name in ('ledger', 'samples', 'checks'):
        module = sys.modules.get(name)
        if module is not None and path not in (
                getattr(module, '__file__', '') or ''):
            del sys.modules[name]
    return sys.path


def _import_artifact_ledger(path):
    _activate_artifact(path)
    return importlib.import_module('ledger')


# ---------------------------------------------------------------------------
# build / validation
# ---------------------------------------------------------------------------

def _repo_root():
    return os.path.dirname(os.path.abspath(__file__))


def build_artifact(release, out_path):
    if release not in RELEASES:
        raise OpsError('BAD_RELEASE')
    version = RELEASES[release]
    root = _repo_root()
    payload = {}
    for relative in SOURCE_FILES:
        source = os.path.join(root, relative)
        try:
            with open(source, 'rb') as handle:
                payload[relative] = handle.read()
        except OSError as exc:
            raise OpsError('SOURCE_UNAVAILABLE %s: %s' % (relative, exc))
    manifest = {
        'schema': 1,
        'kind': 'pointwise-ledger-release',
        'release': release,
        'format': version,
        'files': {relative: sha256_bytes(payload[relative])
                  for relative in SOURCE_FILES},
    }
    manifest_bytes = canonical(manifest).encode('utf-8')
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED,
                         compresslevel=9) as archive:
        for relative in SOURCE_FILES:
            info = zipfile.ZipInfo(relative, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.create_system = 3
            info.external_attr = 0o100644 << 16
            archive.writestr(info, payload[relative])
        info = zipfile.ZipInfo('MANIFEST.json',
                               date_time=(1980, 1, 1, 0, 0, 0))
        info.compress_type = zipfile.ZIP_DEFLATED
        info.create_system = 3
        info.external_attr = 0o100644 << 16
        archive.writestr(info, manifest_bytes)
    raw = b'#!/usr/bin/env python3\n' + buffer.getvalue()
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    atomic_write(out_path, raw, 0o755)
    digest = sha256_bytes(raw)
    atomic_write(out_path + '.manifest.json',
                 canonical(manifest).encode('utf-8') + b'\n')
    atomic_write(out_path + '.sha256', (digest + '\n').encode('utf-8'))
    return {
        'artifact': os.path.abspath(out_path),
        'release': release,
        'format': version,
        'sha256': digest,
        'manifest': os.path.abspath(out_path) + '.manifest.json',
        'sha256_file': os.path.abspath(out_path) + '.sha256',
        'files': manifest['files'],
    }


def validate_artifact(path, expected_release=None):
    try:
        with open(path, 'rb') as handle:
            head = handle.read(4)
    except OSError as exc:
        raise OpsError('ARTIFACT_UNREADABLE: %s' % exc)
    if not head:
        raise OpsError('ARTIFACT_EMPTY')
    try:
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
            if len(names) != len(set(names)):
                raise OpsError('ARTIFACT_DUPLICATE_ENTRY')
            for name in names:
                if (name.startswith('/') or name.startswith('\\')
                        or '..' in name.split('/') or '\x00' in name):
                    raise OpsError('ARTIFACT_UNSAFE_PATH')
            if 'MANIFEST.json' not in names:
                raise OpsError('ARTIFACT_MANIFEST_MISSING')
            try:
                manifest = json.loads(
                    archive.read('MANIFEST.json').decode('utf-8'))
            except (ValueError, UnicodeError, KeyError):
                raise OpsError('ARTIFACT_MANIFEST_CORRUPT')
            if not isinstance(manifest, dict) or manifest.get('schema') != 1:
                raise OpsError('ARTIFACT_MANIFEST_CORRUPT')
            if manifest.get('kind') != 'pointwise-ledger-release':
                raise OpsError('ARTIFACT_MANIFEST_CORRUPT')
            release = manifest.get('release')
            if release not in RELEASES:
                raise OpsError('ARTIFACT_RELEASE_UNKNOWN')
            if manifest.get('format') != RELEASES[release]:
                raise OpsError('ARTIFACT_FORMAT_BINDING_MISMATCH')
            if expected_release is not None and release != expected_release:
                raise OpsError('ARTIFACT_RELEASE_MISMATCH')
            declared = manifest.get('files')
            if not isinstance(declared, dict):
                raise OpsError('ARTIFACT_MANIFEST_CORRUPT')
            actual = set(names) - {'MANIFEST.json'}
            if actual != set(declared):
                raise OpsError('ARTIFACT_CONTENT_LIST_MISMATCH')
            for name in sorted(actual):
                try:
                    content = archive.read(name)
                except (KeyError, zipfile.BadZipFile, OSError):
                    raise OpsError('ARTIFACT_CONTENT_CORRUPT')
                if sha256_bytes(content) != declared[name]:
                    raise OpsError('ARTIFACT_CONTENT_DIGEST_MISMATCH')
            if 'ledger.py' not in declared:
                raise OpsError('ARTIFACT_LEDGER_MISSING')
    except zipfile.BadZipFile:
        raise OpsError('ARTIFACT_NOT_A_ZIP')
    except OpsError:
        raise
    except Exception as exc:
        raise OpsError('ARTIFACT_CORRUPT: %s' % exc)
    try:
        digest = sha256_file(path)
    except OSError as exc:
        raise OpsError('ARTIFACT_UNREADABLE: %s' % exc)
    return {
        'path': os.path.abspath(path),
        'sha256': digest,
        'release': release,
        'format': RELEASES[release],
        'manifest': manifest,
    }


# ---------------------------------------------------------------------------
# database helpers (the ledger core does the conversion)
# ---------------------------------------------------------------------------

def _migrate_database(path, target, package):
    ledger = _import_artifact_ledger(package)
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    connection = ledger.connect(path)
    try:
        try:
            current = ledger.schema(connection)
        except (ledger.LedgerError, sqlite3.OperationalError):
            ledger.initialize(connection, target)
            current = ledger.schema(connection)
        if current != target:
            ledger.migrate(connection, target)
            current = ledger.schema(connection)
        if current != target:
            raise OpsError('DB_FORMAT_MISMATCH')
    finally:
        connection.close()


def _ensure_database(path, state, package):
    _migrate_database(path, int(state['format']), package)


def _write_if_different(path, content, mode=0o644):
    try:
        with open(path, 'rb') as handle:
            if handle.read() == content:
                return path
    except OSError:
        pass
    atomic_write(path, content, mode)
    return path


def _install_entry(state_dir):
    source = os.path.abspath(__file__)
    with open(source, 'rb') as handle:
        content = handle.read()
    target = os.path.join(state_dir, 'bin', 'ops.py')
    _write_if_different(target, content, 0o755)
    # Keep the buildable source snapshot next to the installed manager so a
    # target machine can rebuild either release after the repository and the
    # original build directory are gone.
    root = _repo_root()
    for relative in SOURCE_FILES:
        origin = os.path.join(root, relative)
        destination = os.path.join(state_dir, 'bin', relative)
        if os.path.abspath(origin) == os.path.abspath(destination):
            continue
        try:
            with open(origin, 'rb') as handle:
                payload = handle.read()
        except OSError:
            continue
        _write_if_different(destination, payload, 0o644)
    return target


def _pause_point(state_dir, name):
    wanted = os.environ.get('POINTWISE_TEST_PAUSE_AT', '')
    if not wanted or wanted not in (name, 'all'):
        return
    marker = os.path.join(state_dir, '.pause-' + name)
    with open(marker, 'w', encoding='utf-8') as handle:
        handle.write(str(os.getpid()))
        handle.flush()
    seconds = float(os.environ.get('POINTWISE_TEST_PAUSE_SECONDS', '10'))
    time.sleep(seconds)


# ---------------------------------------------------------------------------
# publication
# ---------------------------------------------------------------------------

def publish(state_dir, operation, expect, request_id, artifact=None):
    if operation not in ('promote', 'rollback'):
        raise OpsError('BAD_OPERATION')
    if not isinstance(request_id, str) or not request_id:
        raise OpsError('BAD_REQUEST_ID')
    if int(expect) < 0:
        raise OpsError('BAD_EXPECT')
    os.makedirs(state_dir, exist_ok=True)
    with StateLock(state_dir, blocking=True):
        state = load_state(state_dir)
        generation = state_generation(state)
        requests = dict(state.get('requests', {})) if state else {}

        if request_id in requests:
            record = requests[request_id]
            intent = {'op': operation, 'expect': int(expect)}
            if operation == 'promote':
                info = validate_artifact(artifact)
                intent['package_sha256'] = info['sha256']
            if record.get('intent') != intent:
                raise OpsError('REQUEST_ID_CONFLICT')
            return dict(record['result'])

        if int(expect) != generation:
            raise OpsError('EXPECT_MISMATCH expected=%d current=%d'
                           % (int(expect), generation))

        if operation == 'promote':
            info = validate_artifact(artifact)
            release = info['release']
            target_format = info['format']
            digest = info['sha256']
            stored = package_path(state_dir, digest)
            needs_copy = True
            try:
                if sha256_file(stored) == digest:
                    validate_artifact(stored, expected_release=release)
                    needs_copy = False
            except (OpsError, OSError):
                needs_copy = True
            if needs_copy:
                with open(artifact, 'rb') as handle:
                    content = handle.read()
                if sha256_bytes(content) != digest:
                    raise OpsError('ARTIFACT_CHANGED_WHILE_READING')
                atomic_write(stored, content, 0o444)
            validate_artifact(stored, expected_release=release)
            intent = {'op': 'promote', 'expect': int(expect),
                      'package_sha256': digest}
        else:
            if state is None or len(state.get('history', [])) < 2:
                raise OpsError('NO_PREVIOUS_RELEASE')
            previous = state['history'][-2]
            release = previous['release']
            target_format = int(previous['format'])
            digest = previous['package_sha256']
            stored = package_path(state_dir, digest)
            validate_artifact(stored, expected_release=release)
            intent = {'op': 'rollback', 'expect': int(expect)}

        target_generation = generation + 1
        result = {
            'generation': target_generation,
            'release': release,
            'format': target_format,
            'request_id': request_id,
            'operation': operation,
            'package_sha256': digest,
        }
        entry_path = _install_entry(state_dir)
        result['management_entry'] = entry_path

        history = list(state.get('history', [])) if state else []
        history.append({
            'generation': target_generation,
            'release': release,
            'format': target_format,
            'package_sha256': digest,
            'request_id': request_id,
            'operation': operation,
        })
        requests[request_id] = {'intent': intent, 'result': result}
        next_state = {
            'schema': 1,
            'generation': target_generation,
            'release': release,
            'format': target_format,
            'package': {
                'sha256': digest,
                'release': release,
                'format': target_format,
            },
            'db': DB_NAME,
            'history': history,
            'requests': requests,
            'updated_by': {'operation': operation, 'request_id': request_id,
                           'pid': os.getpid()},
        }
        pending = {
            'schema': 1,
            'operation': operation,
            'request_id': request_id,
            'expect': int(expect),
            'target_generation': target_generation,
            'package_sha256': digest,
            'next_state': next_state,
        }
        atomic_write(os.path.join(state_dir, PENDING_FILE),
                     canonical(pending).encode('utf-8') + b'\n')
        try:
            _pause_point(state_dir, 'after-pending')

            # Setting the journal already makes workers answer 503.  Taking
            # the exclusive gate now waits for any write that crossed the old
            # check, so every admitted write is included in the migration.
            with StateLock(state_dir, blocking=True, name='write.lock'):
                _migrate_database(db_path(state_dir), target_format, stored)
                _pause_point(state_dir, 'after-migrate')
                atomic_write(os.path.join(state_dir, STATE_FILE),
                             canonical(next_state).encode('utf-8') + b'\n')
        except BaseException:
            # A regular (non-SIGKILL) failure is rolled back to the old
            # generation before the journal is removed.  A SIGKILL leaves the
            # journal for recover() to complete.
            try:
                current = load_state(state_dir)
                if state_generation(current) <= generation:
                    if state is not None:
                        committed = package_path(
                            state_dir, state['package']['sha256'])
                        _ensure_database(db_path(state_dir, state), state,
                                         committed)
                    try:
                        os.unlink(os.path.join(state_dir, PENDING_FILE))
                    except FileNotFoundError:
                        pass
                    _fsync_dir(state_dir)
            except BaseException:
                pass
            raise
        _pause_point(state_dir, 'after-commit')

        try:
            os.unlink(os.path.join(state_dir, PENDING_FILE))
        except FileNotFoundError:
            pass
        _fsync_dir(state_dir)
        return result


def recover(state_dir, log=None):
    """Complete or safely abandon an interrupted publication."""
    os.makedirs(state_dir, exist_ok=True)
    with StateLock(state_dir, blocking=True):
        state = load_state(state_dir)
        pending = load_pending(state_dir)
        if pending is not None:
            target_generation = int(pending['target_generation'])
            digest = pending['package_sha256']
            target_state = pending['next_state']
            stored = package_path(state_dir, digest)
            try:
                validate_artifact(stored,
                                  expected_release=target_state['release'])
            except OpsError as exc:
                # The package cannot be trusted.  If the conversion already
                # happened, convert back with the committed package so the
                # visible generation and database format agree.  No state is
                # advanced for a damaged package.
                if state is not None:
                    committed = package_path(
                        state_dir, state['package']['sha256'])
                    validate_artifact(committed,
                                      expected_release=state['release'])
                    _ensure_database(db_path(state_dir, state), state,
                                     committed)
                try:
                    os.unlink(os.path.join(state_dir, PENDING_FILE))
                except FileNotFoundError:
                    pass
                if log:
                    log('recovery abandoned damaged pending: %s' % exc)
                return load_state(state_dir)
            _migrate_database(db_path(state_dir, target_state),
                              int(target_state['format']), stored)
            current_generation = state_generation(state)
            if current_generation < target_generation:
                if state is not None and current_generation != target_generation - 1:
                    raise OpsError('PENDING_GENERATION_GAP')
                atomic_write(os.path.join(state_dir, STATE_FILE),
                             canonical(target_state).encode('utf-8') + b'\n')
            if log:
                log('recovered interrupted publication to generation %d'
                    % target_generation)
            try:
                os.unlink(os.path.join(state_dir, PENDING_FILE))
            except FileNotFoundError:
                pass
            _fsync_dir(state_dir)
        state = load_state(state_dir)
        if state is not None:
            stored = package_path(state_dir, state['package']['sha256'])
            validate_artifact(stored, expected_release=state['release'])
            _ensure_database(db_path(state_dir, state), state, stored)
        return state


# ---------------------------------------------------------------------------
# supervisor
# ---------------------------------------------------------------------------

def _make_listener(host, port, timeout=60.0):
    deadline = time.time() + timeout
    last = None
    while True:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if hasattr(socket, 'SO_REUSEPORT'):
                try:
                    listener.setsockopt(socket.SOL_SOCKET,
                                        socket.SO_REUSEPORT, 1)
                except OSError:
                    pass
            listener.bind((host, port))
            listener.listen(128)
            listener.setblocking(True)
            return listener
        except OSError as exc:
            last = exc
            listener.close()
            if exc.errno not in (errno.EADDRINUSE, errno.EACCES):
                raise OpsError('BIND_FAILED: %s' % exc)
            if time.time() >= deadline:
                raise OpsError('BIND_FAILED: %s' % (last,))
            time.sleep(0.05)


def _close_entry(entry, command=b'close-listen\n'):
    descriptor = entry.get('control')
    if descriptor is not None:
        try:
            os.write(descriptor, command)
        except OSError:
            pass


class Supervisor:
    def __init__(self, state_dir, host, port):
        self.state_dir = state_dir
        self.host = host
        self.port = port
        self.entries = []
        self.current = None
        self.stop = False
        self.current_generation = None
        self.log_path = os.path.join(state_dir, 'logs', 'supervisor.log')

    def log(self, message):
        os.makedirs(os.path.dirname(self.log_path), exist_ok=True)
        line = '%s %s\n' % (time.strftime('%Y-%m-%dT%H:%M:%S'),
                            message)
        with open(self.log_path, 'a', encoding='utf-8') as handle:
            handle.write(line)

    def _spawn(self, state):
        generation = int(state['generation'])
        release = state['release']
        version = int(state['format'])
        digest = state['package']['sha256']
        package = package_path(self.state_dir, digest)
        validate_artifact(package, expected_release=release)
        listener = _make_listener(self.host, self.port)
        control_read, control_write = os.pipe()
        os.makedirs(os.path.join(self.state_dir, 'logs'), exist_ok=True)
        log_file = open(os.path.join(
            self.state_dir, 'logs', 'worker-%d.log' % generation),
            'ab', buffering=0)
        command = [
            sys.executable, '-B', package, '--worker',
            '--db', db_path(self.state_dir, state),
            '--release', release,
            '--format', str(version),
            '--generation', str(generation),
            '--state', self.state_dir,
            '--host', self.host,
            '--port', str(self.port),
            '--listen-fd', str(listener.fileno()),
            '--control-fd', str(control_read),
        ]
        environment = dict(os.environ)
        environment['PYTHONUNBUFFERED'] = '1'
        try:
            process = subprocess.Popen(
                command, stdin=subprocess.DEVNULL, stdout=log_file,
                stderr=log_file, close_fds=True,
                pass_fds=(listener.fileno(), control_read),
                env=environment)
        except BaseException:
            os.close(control_read)
            os.close(control_write)
            listener.close()
            log_file.close()
            raise
        os.close(control_read)
        log_file.close()
        listener.close()
        entry = {
            'generation': generation,
            'process': process,
            'control': control_write,
            'superseded': False,
        }
        self.log('started worker generation %d pid %d'
                 % (generation, process.pid))
        return entry

    def _stop_entry(self, entry):
        _close_entry(entry)
        if entry.get('control') is not None:
            try:
                os.close(entry['control'])
            except OSError:
                pass
            entry['control'] = None

    def _start_current(self, state):
        entry = self._spawn(state)
        self.entries.append(entry)
        self.current = entry
        self.current_generation = int(state['generation'])
        return entry

    def switch(self, state):
        old = self.current
        new = self._start_current(state)
        if old is not None:
            old['superseded'] = True
            self._stop_entry(old)
        try:
            _write_supervisor_record(self.state_dir,
                                     int(state['generation']))
        except OSError:
            pass
        self.log('generation %d now accepting' % int(state['generation']))
        return new

    def reap(self):
        remaining = []
        for entry in self.entries:
            code = entry['process'].poll()
            if code is None:
                remaining.append(entry)
                continue
            self.log('worker generation %d pid %d exited %s'
                     % (entry['generation'], entry['process'].pid, code))
            if not entry.get('superseded'):
                self._stop_entry(entry)
        self.entries = remaining

    def run(self):
        state = recover(self.state_dir, self.log)
        if state is None or state_generation(state) < 1:
            raise OpsError('NOT_INSTALLED')
        self._start_current(state)
        while not self.stop:
            try:
                if os.path.exists(os.path.join(self.state_dir, PENDING_FILE)):
                    state = recover(self.state_dir, self.log)
                current = load_state(self.state_dir)
                if current is not None:
                    if int(current['generation']) != self.current_generation:
                        self.switch(current)
                self.reap()
                if (self.current is not None
                        and self.current['process'].poll() is not None
                        and not self.current.get('superseded')
                        and not self.stop):
                    self.log('current worker died; restarting')
                    self._start_current(load_state(self.state_dir))
            except OpsError as exc:
                self.log('supervisor recovery error: %s' % exc)
            except (OSError, ValueError, KeyError, TypeError) as exc:
                self.log('supervisor transient error: %s' % exc)
            except Exception as exc:
                self.log('supervisor unexpected error: %s' % exc)
            time.sleep(0.05)
        for entry in self.entries:
            if not entry.get('superseded'):
                self._stop_entry(entry)
            else:
                if entry.get('control') is not None:
                    try:
                        os.close(entry['control'])
                    except OSError:
                        pass
                    entry['control'] = None
        self.log('supervisor stopped')


def _write_supervisor_record(state_dir, generation):
    record = {'pid': os.getpid(), 'generation': generation,
              'started': time.strftime('%Y-%m-%dT%H:%M:%S')}
    atomic_write(os.path.join(state_dir, 'run', 'supervisor.json'),
                 canonical(record).encode('utf-8') + b'\n')


def cmd_run(args):
    if args.host != '127.0.0.1':
        raise OpsError('HOST_MUST_BE_127.0.0.1')
    state_dir = os.path.abspath(args.state)
    os.makedirs(state_dir, exist_ok=True)
    try:
        run_lock = StateLock(state_dir, blocking=False, name='run.lock')
        run_lock.__enter__()
    except BlockingIOError:
        print(canonical({'status': 'already-running', 'state': state_dir}))
        return 0
    try:
        supervisor = Supervisor(state_dir, args.host, int(args.port))

        def on_signal(signum, frame):
            supervisor.stop = True

        signal.signal(signal.SIGTERM, on_signal)
        signal.signal(signal.SIGINT, on_signal)
        state = recover(state_dir, supervisor.log)
        if state is None or state_generation(state) < 1:
            raise OpsError('NOT_INSTALLED')
        _write_supervisor_record(state_dir, state_generation(state))
        print(canonical({'status': 'running', 'state': state_dir,
                         'host': args.host, 'port': int(args.port)}))
        sys.stdout.flush()
        supervisor.run()
        return 0
    finally:
        run_lock.__exit__(None, None, None)


# ---------------------------------------------------------------------------
# verification / demonstration
# ---------------------------------------------------------------------------

def _artifact_for_format(state, state_dir, version):
    for item in reversed(state.get('history', [])):
        if int(item['format']) == version:
            path = package_path(state_dir, item['package_sha256'])
            validate_artifact(path, expected_release=item['release'])
            return path
    return None


def _obtain_artifacts(state, state_dir, workdir):
    built = {}
    root = _repo_root()
    if all(os.path.exists(os.path.join(root, name)) for name in SOURCE_FILES):
        for release in ('r1', 'r2'):
            path = os.path.join(workdir, 'artifact-%s.pyz' % release)
            build_artifact(release, path)
            built[RELEASES[release]] = path
    else:
        for version in (1, 2):
            path = _artifact_for_format(state, state_dir, version)
            if path is not None:
                built[version] = path
    if not built:
        raise OpsError('NO_USABLE_ARTIFACT')
    _activate_artifact(built[min(built)])
    return built


def _real_ledger_session(package, db, version):
    ledger = _import_artifact_ledger(package)
    connection = ledger.connect(db)
    return ledger, connection


def _selftest_publication(state, state_dir, workdir):
    artifacts = _obtain_artifacts(state, state_dir, workdir)
    samples = importlib.import_module('samples')
    first_version = min(artifacts)
    second_version = max(artifacts)

    test_state = os.path.join(workdir, 'state-install')
    first = artifacts[first_version]
    second = artifacts[second_version]
    result_one = publish(test_state, 'promote', 0, 'verify-promote-1', first)
    assert result_one['generation'] == 1
    assert os.path.isabs(result_one['management_entry'])

    ledger, connection = _real_ledger_session(
        first, db_path(test_state), first_version)
    try:
        ledger.ingest(connection, first_version, samples.INITIAL)
        before = ledger.receipts(connection)
        assert ledger.snapshot(connection, first_version) == samples.INITIAL_EXPECTED
    finally:
        connection.close()

    # A second successful publication always advances the generation, even
    # when only one format happened to be available for the self-test.
    result_two = publish(test_state, 'promote', 1, 'verify-promote-2', second)
    assert result_two['generation'] == 2
    ledger, connection = _real_ledger_session(
        second, db_path(test_state), second_version)
    try:
        assert ledger.snapshot(connection, second_version) == samples.INITIAL_EXPECTED
        ledger.ingest(connection, second_version, samples.ARRIVAL)
        assert ledger.snapshot(connection, second_version) == samples.FINAL_EXPECTED
        receipts_after = ledger.receipts(connection)
    finally:
        connection.close()

    result_three = publish(test_state, 'rollback', 2, 'verify-rollback-1')
    assert result_three['generation'] == 3
    assert result_three['format'] == first_version
    ledger, connection = _real_ledger_session(
        first, db_path(test_state), first_version)
    try:
        assert ledger.snapshot(connection, first_version) == samples.FINAL_EXPECTED
        assert ledger.receipts(connection) == receipts_after
        assert len(before) >= 1
        exported = list(ledger.export(connection, first_version))
        assert exported == sorted(
            samples.INITIAL['rows'] + samples.ARRIVAL['rows'],
            key=lambda row: row['id'])
    finally:
        connection.close()

    replay = publish(test_state, 'promote', 0, 'verify-promote-1', first)
    assert replay == result_one
    assert state_generation(load_state(test_state)) == 3

    if second_version != first_version:
        # Same request id with a different package digest is a different
        # intent, even though the generation check would also fail.
        try:
            publish(test_state, 'promote', 0, 'verify-promote-1', second)
            raise AssertionError('expected request conflict')
        except OpsError as exc:
            assert str(exc) == 'REQUEST_ID_CONFLICT'

    # Concurrent same-expect requests: exactly one may commit.
    race_state = os.path.join(workdir, 'state-race')
    commands = []
    for suffix in ('a', 'b'):
        commands.append([
            sys.executable, '-B', os.path.abspath(__file__), 'promote',
            '--state', race_state, '--artifact', first,
            '--expect', '0', '--request-id', 'verify-race-' + suffix,
        ])
    processes = [subprocess.Popen(command, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
                 for command in commands]
    outcomes = [process.communicate()[0] for process in processes]
    codes = [process.returncode for process in processes]
    assert codes.count(0) == 1, (codes, outcomes)
    race = load_state(race_state)
    assert state_generation(race) == 1
    assert len([key for key in race['requests'] if key.startswith('verify-race-')]) == 1

    # A damaged artifact is rejected without touching the committed state.
    damaged = os.path.join(workdir, 'damaged.pyz')
    with open(first, 'rb') as source:
        raw = source.read()
    with open(damaged, 'wb') as target:
        target.write(raw[:len(raw) // 2])
    try:
        validate_artifact(damaged)
        raise AssertionError('damaged artifact accepted')
    except OpsError:
        pass
    good_generation = state_generation(load_state(race_state))
    try:
        publish(race_state, 'promote', 1, 'verify-damaged', damaged)
        raise AssertionError('damaged artifact published')
    except OpsError:
        pass
    assert state_generation(load_state(race_state)) == good_generation

    # Kill a promotion at the durable journal boundary, then recover.
    kill_state = os.path.join(workdir, 'state-kill')
    environment = dict(os.environ)
    environment['POINTWISE_TEST_PAUSE_AT'] = 'after-pending'
    environment['POINTWISE_TEST_PAUSE_SECONDS'] = '30'
    process = subprocess.Popen(
        [sys.executable, '-B', os.path.abspath(__file__), 'promote',
         '--state', kill_state, '--artifact', first, '--expect', '0',
         '--request-id', 'verify-killed'],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=environment)
    marker = os.path.join(kill_state, '.pause-after-pending')
    deadline = time.time() + 15
    while not os.path.exists(marker) and time.time() < deadline:
        if process.poll() is not None:
            break
        time.sleep(0.02)
    assert os.path.exists(marker), 'pause marker never appeared'
    process.send_signal(signal.SIGKILL)
    process.wait()
    recovered = recover(kill_state)
    assert recovered is not None
    assert state_generation(recovered) == 1
    assert int(recovered['format']) == first_version
    ledger, connection = _real_ledger_session(
        first, db_path(kill_state, recovered), first_version)
    try:
        ledger.ingest(connection, first_version, samples.INITIAL)
        assert ledger.snapshot(connection, first_version) == samples.INITIAL_EXPECTED
    finally:
        connection.close()
    print('RELEASE_RECOVERY_OK')


def cmd_verify(args):
    state_dir = os.path.abspath(args.state)
    state = load_state(state_dir)
    if state is None or state_generation(state) < 1:
        raise OpsError('NOT_INSTALLED')
    package = package_path(state_dir, state['package']['sha256'])
    validate_artifact(package, expected_release=state['release'])
    _ensure_database(db_path(state_dir, state), state, package)
    _activate_artifact(package)
    checks = importlib.import_module('checks')
    checks.check()
    workdir = tempfile.mkdtemp(prefix='pointwise-verify-')
    try:
        _selftest_publication(state, state_dir, workdir)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
    print('RELEASE_VERIFY_OK')


def cmd_demo(args):
    root = _repo_root()
    if not all(os.path.exists(os.path.join(root, name))
               for name in SOURCE_FILES):
        raise OpsError('DEMO_REQUIRES_SOURCE')
    workdir = os.path.abspath(args.workdir)
    os.makedirs(workdir, exist_ok=True)
    state_dir = os.path.join(workdir, 'demo-state')
    if os.path.exists(state_dir):
        shutil.rmtree(state_dir)
    artifacts = {}
    for release in ('r1', 'r2'):
        path = os.path.join(workdir, 'artifact-%s.pyz' % release)
        build_artifact(release, path)
        artifacts[RELEASES[release]] = path
    _activate_artifact(artifacts[1])
    samples = importlib.import_module('samples')
    result = publish(state_dir, 'promote', 0, 'demo-r1', artifacts[1])
    ledger, connection = _real_ledger_session(
        artifacts[1], db_path(state_dir), 1)
    try:
        ledger.ingest(connection, 1, samples.INITIAL)
        snapshot = ledger.snapshot(connection, 1)
    finally:
        connection.close()
    print(canonical({'phase': 'r1', 'generation': result['generation'],
                     'release': result['release'],
                     'snapshot': snapshot}))
    result = publish(state_dir, 'promote', 1, 'demo-r2', artifacts[2])
    ledger, connection = _real_ledger_session(
        artifacts[2], db_path(state_dir), 2)
    try:
        ledger.ingest(connection, 2, samples.ARRIVAL)
        snapshot = ledger.snapshot(connection, 2)
        receipts = ledger.receipts(connection)
    finally:
        connection.close()
    print(canonical({'phase': 'r2-arrival', 'generation': result['generation'],
                     'release': result['release'], 'snapshot': snapshot}))
    result = publish(state_dir, 'rollback', 2, 'demo-rollback')
    ledger, connection = _real_ledger_session(
        artifacts[1], db_path(state_dir), 1)
    try:
        snapshot = ledger.snapshot(connection, 1)
        receipts = ledger.receipts(connection)
    finally:
        connection.close()
    print(canonical({'phase': 'r1-after-rollback',
                     'generation': result['generation'],
                     'release': result['release'], 'snapshot': snapshot,
                     'receipts': receipts}))


def cmd_bootstrap(args):
    state_dir = os.path.abspath(args.state)
    state = load_state(state_dir)
    if state is not None and state_generation(state) >= 1:
        result = recover(state_dir)
        print(canonical({'status': 'already-installed',
                         'generation': result['generation'],
                         'release': result['release'],
                         'format': result['format'],
                         'management_entry':
                             os.path.join(state_dir, 'bin', 'ops.py')}))
        return 0
    artifacts = (os.path.abspath(args.build_dir) if args.build_dir
                 else os.path.join(state_dir, 'artifacts'))
    os.makedirs(artifacts, exist_ok=True)
    path = os.path.join(artifacts, 'release-%s.pyz' % args.release)
    build_artifact(args.release, path)
    result = publish(state_dir, 'promote', 0,
                     args.request_id or 'initial-install', path)
    print(canonical({'status': 'installed', **result}))
    return 0


# ---------------------------------------------------------------------------
# LaunchAgent
# ---------------------------------------------------------------------------

def _agent_label(state_dir):
    digest = sha256_bytes(os.path.abspath(state_dir).encode('utf-8'))
    return 'com.pointwise.ledger.' + digest[:12]


def _agents_dir():
    return os.environ.get(
        'POINTWISE_LAUNCH_AGENTS_DIR',
        os.path.join(os.path.expanduser('~'), 'Library', 'LaunchAgents'))


def _xml_escape(value):
    return (value.replace('&', '&amp;').replace('<', '&lt;')
            .replace('>', '&gt;').replace('"', '&quot;'))


def _plist(label, arguments, environment, workdir, stdout_path, stderr_path):
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" '
        '"http://www.apple.com/DTDs/PropertyList-1.0.dtd">',
        '<plist version="1.0">',
        '<dict>',
        '  <key>Label</key>',
        '  <string>%s</string>' % _xml_escape(label),
        '  <key>ProgramArguments</key>',
        '  <array>',
    ]
    for argument in arguments:
        lines.append('    <string>%s</string>' % _xml_escape(argument))
    lines += [
        '  </array>',
        '  <key>RunAtLoad</key>',
        '  <true/>',
        '  <key>KeepAlive</key>',
        '  <true/>',
        '  <key>WorkingDirectory</key>',
        '  <string>%s</string>' % _xml_escape(workdir),
        '  <key>StandardOutPath</key>',
        '  <string>%s</string>' % _xml_escape(stdout_path),
        '  <key>StandardErrorPath</key>',
        '  <string>%s</string>' % _xml_escape(stderr_path),
        '  <key>EnvironmentVariables</key>',
        '  <dict>',
    ]
    for key, value in sorted(environment.items()):
        lines.append('    <key>%s</key>' % _xml_escape(key))
        lines.append('    <string>%s</string>' % _xml_escape(value))
    lines += [
        '  </dict>',
        '</dict>',
        '</plist>',
        '',
    ]
    return '\n'.join(lines)


def _launchctl(*arguments):
    try:
        completed = subprocess.run(['launchctl'] + list(arguments),
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        return completed.returncode == 0, completed.stderr.decode(
            'utf-8', 'replace')
    except FileNotFoundError:
        return False, 'launchctl not available'


def cmd_agent_install(args):
    state_dir = os.path.abspath(args.state)
    state = load_state(state_dir)
    if state is None or state_generation(state) < 1:
        raise OpsError('NOT_INSTALLED')
    entry = _install_entry(state_dir)
    interpreter = os.path.realpath(sys.executable) if sys.executable else (
        shutil.which('python3'))
    if not interpreter:
        raise OpsError('PYTHON_INTERPRETER_NOT_FOUND')
    label = _agent_label(state_dir)
    plist_path = os.path.join(_agents_dir(), label + '.plist')
    logs = os.path.join(state_dir, 'logs')
    os.makedirs(logs, exist_ok=True)
    arguments = [interpreter, '-B', entry, 'run', '--state', state_dir,
                 '--host', '127.0.0.1', '--port', str(int(args.port))]
    content = _plist(label, arguments,
                     {'PYTHONUNBUFFERED': '1', 'POINTWISE_STATE': state_dir,
                      'POINTWISE_HOST': '127.0.0.1',
                      'POINTWISE_PORT': str(int(args.port))},
                     state_dir,
                     os.path.join(logs, 'launchd.out.log'),
                     os.path.join(logs, 'launchd.err.log'))
    os.makedirs(os.path.dirname(plist_path), exist_ok=True)
    atomic_write(plist_path, content.encode('utf-8'))
    registered = False
    message = 'launchd registration only applies on macOS'
    if sys.platform == 'darwin':
        uid = str(os.getuid())
        _launchctl('bootout', 'gui/%s/%s' % (uid, label))
        ok, detail = _launchctl('bootstrap', 'gui/%s' % uid, plist_path)
        registered = ok
        message = 'bootstrapped' if ok else detail
    print(canonical({'label': label, 'plist': plist_path,
                     'management_entry': entry,
                     'program_arguments': arguments,
                     'registered': registered, 'message': message}))
    return 0


def _managed_pid(state_dir):
    record = read_json(os.path.join(state_dir, 'run', 'supervisor.json'))
    if not record or not isinstance(record.get('pid'), int):
        return None
    pid = record['pid']
    try:
        with open('/proc/%d/cmdline' % pid, 'rb') as handle:
            command = handle.read().decode('utf-8', 'replace')
    except OSError:
        try:
            completed = subprocess.run(
                ['ps', '-axo', 'pid=,command='], stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
            command = ''
            for line in completed.stdout.decode('utf-8', 'replace').splitlines():
                if line.strip().startswith(str(pid) + ' '):
                    command = line
                    break
        except (OSError, FileNotFoundError):
            return None
    if 'ops.py' in command and state_dir in command and ' run ' in command:
        return pid
    return None


def cmd_agent_uninstall(args):
    state_dir = os.path.abspath(args.state)
    label = _agent_label(state_dir)
    plist_path = os.path.join(_agents_dir(), label + '.plist')
    removed_plist = False
    registered = False
    if sys.platform == 'darwin':
        uid = str(os.getuid())
        ok, _ = _launchctl('bootout', 'gui/%s/%s' % (uid, label))
        registered = ok
    try:
        os.unlink(plist_path)
        removed_plist = True
    except FileNotFoundError:
        pass
    stopped = False
    pid = _managed_pid(state_dir)
    if pid is not None:
        try:
            os.kill(pid, signal.SIGTERM)
            stopped = True
        except OSError:
            pass
    print(canonical({'label': label, 'plist': plist_path,
                     'plist_removed': removed_plist,
                     'launchctl_bootout': registered,
                     'supervisor_signalled': stopped,
                     'data_preserved': True,
                     'packages_preserved': True}))
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _print_error(exc):
    print(canonical({'error': str(exc)}))


def main(argv=None):
    parser = argparse.ArgumentParser(prog='ops.py')
    subparsers = parser.add_subparsers(dest='command', required=True)

    build = subparsers.add_parser('build')
    build.add_argument('--release', choices=sorted(RELEASES), required=True)
    build.add_argument('--out', required=True)

    promote = subparsers.add_parser('promote')
    promote.add_argument('--state', required=True)
    promote.add_argument('--artifact', required=True)
    promote.add_argument('--expect', type=int, required=True)
    promote.add_argument('--request-id', required=True)

    rollback = subparsers.add_parser('rollback')
    rollback.add_argument('--state', required=True)
    rollback.add_argument('--expect', type=int, required=True)
    rollback.add_argument('--request-id', required=True)

    run = subparsers.add_parser('run')
    run.add_argument('--state', required=True)
    run.add_argument('--host', default='127.0.0.1')
    run.add_argument('--port', type=int, default=8765)

    verify = subparsers.add_parser('verify')
    verify.add_argument('--state', required=True)

    demo = subparsers.add_parser('demo')
    demo.add_argument('--workdir', default='var/demo')

    bootstrap = subparsers.add_parser('bootstrap')
    bootstrap.add_argument('--state', required=True)
    bootstrap.add_argument('--release', choices=sorted(RELEASES),
                           default='r1')
    bootstrap.add_argument('--request-id', default='initial-install')
    bootstrap.add_argument('--build-dir', default=None)

    agent_install = subparsers.add_parser('agent-install')
    agent_install.add_argument('--state', required=True)
    agent_install.add_argument('--port', type=int, default=8765)

    agent_uninstall = subparsers.add_parser('agent-uninstall')
    agent_uninstall.add_argument('--state', required=True)

    recover_parser = subparsers.add_parser('recover')
    recover_parser.add_argument('--state', required=True)

    status = subparsers.add_parser('status')
    status.add_argument('--state', required=True)

    args = parser.parse_args(argv)
    try:
        if args.command == 'build':
            print(canonical(build_artifact(args.release, args.out)))
            return 0
        if args.command == 'promote':
            print(canonical(publish(os.path.abspath(args.state), 'promote',
                                    args.expect, args.request_id,
                                    os.path.abspath(args.artifact))))
            return 0
        if args.command == 'rollback':
            print(canonical(publish(os.path.abspath(args.state), 'rollback',
                                    args.expect, args.request_id)))
            return 0
        if args.command == 'run':
            return cmd_run(args)
        if args.command == 'verify':
            return cmd_verify(args)
        if args.command == 'demo':
            return cmd_demo(args)
        if args.command == 'bootstrap':
            return cmd_bootstrap(args)
        if args.command == 'agent-install':
            return cmd_agent_install(args)
        if args.command == 'agent-uninstall':
            return cmd_agent_uninstall(args)
        if args.command == 'recover':
            state = recover(os.path.abspath(args.state))
            print(canonical({'recovered': True, 'generation':
                             state_generation(state),
                             'release': state['release'],
                             'format': state['format']}))
            return 0
        if args.command == 'status':
            state = load_state(os.path.abspath(args.state))
            if state is None:
                print(canonical({'installed': False, 'generation': 0}))
            else:
                print(canonical({'installed': True,
                                 'generation': state['generation'],
                                 'release': state['release'],
                                 'format': state['format'],
                                 'package_sha256':
                                     state['package']['sha256'],
                                 'requests': len(state['requests'])}))
            return 0
        raise OpsError('UNKNOWN_COMMAND')
    except OpsError as exc:
        _print_error(exc)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == '__main__':
    raise SystemExit(main())
