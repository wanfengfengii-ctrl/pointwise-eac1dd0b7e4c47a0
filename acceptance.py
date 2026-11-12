#!/usr/bin/env python3
"""Re-runnable acceptance evidence for the pointwise release manager.

Every scenario uses independently started processes, the actual zipapp built
from the repository, and one persistent state directory per scenario.  The
ledger calls are the real ledger module from the installed artifact; no HTTP
or accounting behaviour is simulated.

Usage:
    python3 -B acceptance.py [--workdir DIR] [--port PORT]

The directory is recreated for each run, so the script is idempotent and can
be run repeatedly.  It prints one EVIDENCE line per assertion group and exits
non-zero on the first failure.
"""

import argparse
import json
import os
import plistlib
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.abspath(__file__))
OPS = os.path.join(ROOT, 'ops.py')
RECEIVED = []


def evidence(name, payload):
    line = {'evidence': name, **payload}
    RECEIVED.append(line)
    print(json.dumps(line, ensure_ascii=False, sort_keys=True))


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def ops(*arguments, env=None, timeout=120):
    return subprocess.run(
        [sys.executable, '-B', OPS] + list(arguments),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        env=env, timeout=timeout)


def build(release, path):
    completed = ops('build', '--release', release, '--out', path)
    check(completed.returncode == 0,
          'build %s failed: %s' % (release, completed.stderr))
    return json.loads(completed.stdout)


def promote(state, artifact, expect, request_id, env=None):
    return ops('promote', '--state', state, '--artifact', artifact,
               '--expect', str(expect), '--request-id', request_id, env=env)


def rollback(state, expect, request_id):
    return ops('rollback', '--state', state, '--expect', str(expect),
               '--request-id', request_id)


def load_state(state):
    with open(os.path.join(state, 'state.json'), 'rb') as handle:
        return json.loads(handle.read().decode('utf-8'))


def db_format(state):
    connection = sqlite3.connect(os.path.join(state, 'ledger.sqlite'))
    try:
        return connection.execute('SELECT version FROM format').fetchone()[0]
    finally:
        connection.close()


def free_port():
    listener = socket.socket()
    try:
        listener.bind(('127.0.0.1', 0))
        return listener.getsockname()[1]
    finally:
        listener.close()


def start_run(state, port, log_path):
    log = open(log_path, 'wb')
    process = subprocess.Popen(
        [sys.executable, '-B', OPS, 'run', '--state', state,
         '--host', '127.0.0.1', '--port', str(port)],
        stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
        start_new_session=True)
    return process


def stop_run(process, grace=10):
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def request(port, method, path, body=None, timeout=10):
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode('utf-8')
        headers['Content-Type'] = 'application/json'
    req = urllib.request.Request(
        'http://127.0.0.1:%d%s' % (port, path), data=data,
        headers=headers, method=method)
    try:
        response = urllib.request.urlopen(req, timeout=timeout)
        return response.status, response.read().decode('utf-8')
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode('utf-8')
    except (urllib.error.URLError, ConnectionError, OSError) as exc:
        return None, repr(exc)


def retry_request(port, method, path, body=None, accept=(200,), seconds=20):
    deadline = time.time() + seconds
    last = None
    while time.time() < deadline:
        try:
            status, text = request(port, method, path, body, timeout=5)
            last = (status, text)
            if status in accept:
                return status, text
        except (urllib.error.URLError, ConnectionError, OSError) as exc:
            last = (None, repr(exc))
        time.sleep(0.05)
    return last


def wait_health(port, release, fmt, seconds=20):
    deadline = time.time() + seconds
    last = None
    while time.time() < deadline:
        try:
            status, text = request(port, 'GET', '/health', timeout=3)
            last = (status, text)
            if status == 200:
                body = json.loads(text)
                if body.get('release') == release and body.get('format') == fmt:
                    return body
        except (urllib.error.URLError, ConnectionError, OSError) as exc:
            last = (None, repr(exc))
        time.sleep(0.05)
    raise AssertionError('health never reached %s/%d: %r'
                         % (release, fmt, last))


def open_slow_export(port):
    connection = socket.socket()
    connection.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8192)
    connection.connect(('127.0.0.1', port))
    connection.sendall(b'GET /export HTTP/1.0\r\nHost: x\r\n\r\n')
    buffer = b''
    while b'\r\n\r\n' not in buffer:
        chunk = connection.recv(4096)
        if not chunk:
            raise AssertionError('export closed before headers')
        buffer += chunk
    head, rest = buffer.split(b'\r\n\r\n', 1)
    check(head.split(b'\r\n')[0].startswith(b'HTTP/1.0 200'),
          'export did not start with 200: %r' % head[:80])
    return connection, rest


def drain_export(connection, rest):
    data = rest
    while True:
        try:
            chunk = connection.recv(65536)
        except ConnectionError:
            break
        if not chunk:
            break
        data += chunk
    connection.close()
    return [json.loads(line) for line in data.decode('utf-8').splitlines()
            if line.strip()]


def ingest_with_artifact(artifact, db, version, batch):
    saved_path = list(sys.path)
    saved_modules = {name: sys.modules.pop(name, None)
                     for name in ('ledger', 'samples')}
    try:
        sys.path.insert(0, artifact)
        import ledger
        connection = ledger.connect(db)
        try:
            return ledger.ingest(connection, version, batch)
        finally:
            connection.close()
    finally:
        sys.modules.pop('ledger', None)
        for name, module in saved_modules.items():
            if module is not None:
                sys.modules[name] = module
        sys.path[:] = saved_path


def receivable_count(artifact, db, version):
    saved_path = list(sys.path)
    saved_modules = {name: sys.modules.pop(name, None)
                     for name in ('ledger', 'samples')}
    try:
        sys.path.insert(0, artifact)
        import ledger
        connection = ledger.connect(db)
        try:
            return ledger.receipts(connection)
        finally:
            connection.close()
    finally:
        sys.modules.pop('ledger', None)
        for name, module in saved_modules.items():
            if module is not None:
                sys.modules[name] = module
        sys.path[:] = saved_path


def run_paused(arguments, marker, env_extra, kill=True):
    environment = dict(os.environ)
    environment.update(env_extra)
    process = subprocess.Popen(
        [sys.executable, '-B', OPS] + list(arguments),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        env=environment)
    deadline = time.time() + 20
    while not os.path.exists(marker):
        if process.poll() is not None:
            out, err = process.communicate()
            raise AssertionError('paused command exited early: %s %s'
                                 % (out, err))
        if time.time() > deadline:
            process.kill()
            raise AssertionError('pause marker never appeared: %s' % marker)
        time.sleep(0.02)
    if kill:
        process.kill()
        process.communicate()
    return process


# ---------------------------------------------------------------------------
# scenarios
# ---------------------------------------------------------------------------

def scenario_business(state, port, artifacts, log):
    result = json.loads(promote(state, artifacts['r1'], 0, 's1-install').stdout)
    check(result['generation'] == 1 and result['release'] == 'r1',
          'first install generation')
    check(os.path.isabs(result['management_entry'])
          and os.path.exists(result['management_entry']),
          'management entry installed')
    run = start_run(state, port, log)
    try:
        wait_health(port, 'r1', 1)
        initial = {
            'batch_id': 'initial',
            'rows': [
                {'id': 'm1', 'station': '江北', 'day': '2026-10-31',
                 'amount': 120},
                {'id': 'm2', 'station': '江北', 'day': '2026-10-31',
                 'amount': -20},
                {'id': 'm3', 'station': '南站', 'day': '2026-11-01',
                 'amount': 70},
            ],
        }
        status, body = request(port, 'POST', '/batches', initial)
        check(status == 200 and json.loads(body) == {
            'batch_id': 'initial', 'rows': 3}, 'initial ingest')
        status, body = request(port, 'POST', '/batches', initial)
        check(status == 200 and json.loads(body) == {
            'batch_id': 'initial', 'rows': 3}, 'idempotent replay')
        snapshot = json.loads(request(port, 'GET', '/snapshot')[1])
        check(snapshot['count'] == 3 and snapshot['daily'] == [
            {'day': '2026-10-31', 'station': '江北', 'amount': 100,
             'count': 2},
            {'day': '2026-11-01', 'station': '南站', 'amount': 70,
             'count': 1},
        ], 'initial snapshot')
        conflict = dict(initial)
        conflict = {'batch_id': 'initial',
                    'rows': [dict(row) for row in initial['rows']]}
        conflict['rows'][0]['amount'] += 1
        status, _ = request(port, 'POST', '/batches', conflict)
        check(status == 409, 'conflicting replay is rejected')
        check(json.loads(request(port, 'GET', '/snapshot')[1]) == snapshot,
              'conflict left snapshot unchanged')
        exported = [json.loads(line) for line in
                    request(port, 'GET', '/export')[1].splitlines()]
        check(len(exported) == 3 and [row['id'] for row in exported] ==
              ['m1', 'm2', 'm3'], 'r1 export')
        evidence('business_install', {'generation': 1, 'release': 'r1',
                                      'format': 1, 'count': 3,
                                      'replay': 'same-result',
                                      'conflict': 409})
    finally:
        stop_run(run)
    return initial


def scenario_switch(state, port, artifacts, log):
    bulk = {
        'batch_id': 'bulk',
        'rows': [{'id': 'x%05d' % index, 'station': '甲' if index % 2 else '乙',
                  'day': '2026-11-02', 'amount': 1}
                 for index in range(3000)],
    }
    run = start_run(state, port, log)
    try:
        wait_health(port, 'r1', 1)
        status, body = retry_request(port, 'POST', '/batches', bulk,
                                     accept=(200,), seconds=20)
        check(status == 200 and json.loads(body)['rows'] == 3000,
              'bulk ingest')

        export_connection, rest = open_slow_export(port)
        started = time.time()
        completed = promote(state, artifacts['r2'], 1, 's2-upgrade')
        check(completed.returncode == 0, 'upgrade promote: %s'
              % completed.stderr)
        switch_seconds = time.time() - started
        check(switch_seconds < 10, 'switch waited for slow export')

        arrival = {'batch_id': 'during-r2', 'rows': [
            {'id': 'm4', 'station': '江北', 'day': '2026-11-01',
             'amount': 30}]}
        status, body = retry_request(port, 'POST', '/batches', arrival,
                                     accept=(200,))
        check(status == 200, 'write after switch: %r' % (body,))
        wait_health(port, 'r2', 2)
        snapshot = json.loads(request(port, 'GET', '/snapshot')[1])
        check(snapshot['count'] == 3004, 'r2 snapshot after arrival')

        exported = drain_export(export_connection, rest)
        check(len(exported) == 3003, 'old export retained its snapshot')
        check(len({row['id'] for row in exported}) == 3003,
              'old export has no duplicates')
        check(not any(row['id'] == 'm4' for row in exported),
              'old export excludes post-switch write')

        receipts = receivable_count(artifacts['r2'],
                                    os.path.join(state, 'ledger.sqlite'), 2)
        check(dict(receipts)['bulk'] is not None and
              dict(receipts)['during-r2'] is not None,
              'receipts retained across upgrade')
        evidence('slow_export_switch', {
            'switch_seconds': round(switch_seconds, 3),
            'old_export_rows': len(exported),
            'post_switch_write': 200,
            'health': {'release': 'r2', 'format': 2},
        })
    finally:
        stop_run(run)


def scenario_rollback(state, port, artifacts, log):
    completed = rollback(state, 2, 's3-rollback')
    check(completed.returncode == 0, 'rollback: %s' % completed.stderr)
    result = json.loads(completed.stdout)
    check(result['generation'] == 3 and result['release'] == 'r1',
          'rollback is a new generation')
    run = start_run(state, port, log)
    try:
        wait_health(port, 'r1', 1)
        snapshot = json.loads(request(port, 'GET', '/snapshot')[1])
        check(snapshot['count'] == 3004, 'rollback kept new-version data')
        exported = [json.loads(line) for line in
                    request(port, 'GET', '/export')[1].splitlines()]
        check(any(row['id'] == 'm4' for row in exported),
              'rollback export contains arrival')
        receipts = receivable_count(artifacts['r1'],
                                    os.path.join(state, 'ledger.sqlite'), 1)
        check(set(name for name, _ in receipts) ==
              {'bulk', 'during-r2', 'initial'},
              'rollback preserved every receipt')
        replay = json.loads(promote(state, artifacts['r1'], 0,
                                    's1-install').stdout)
        check(replay['generation'] == 1, 'old request replay returns old result')
        check(load_state(state)['generation'] == 3,
              'old request replay did not move head')
        evidence('rollback_with_new_data', {
            'generation': 3, 'release': 'r1', 'count': 3004,
            'receipts': sorted(name for name, _ in receipts),
            'replayed_head_generation': 3,
        })
    finally:
        stop_run(run)


def scenario_concurrency(workdir, artifacts):
    state = os.path.join(workdir, 'state-concurrency')
    processes = [subprocess.Popen(
        [sys.executable, '-B', OPS, 'promote', '--state', state,
         '--artifact', artifacts['r1'], '--expect', '0',
         '--request-id', 'race-%d' % index],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        for index in range(6)]
    results = [process.communicate() + (process.returncode,)
               for process in processes]
    successes = [result for result in results if result[2] == 0]
    check(len(successes) == 1,
          'exactly one concurrent same-expect promotion may commit')
    final = load_state(state)
    check(final['generation'] == 1 and len(final['requests']) == 1,
          'only one concurrent request committed')

    same = os.path.join(workdir, 'state-same-id')
    processes = [subprocess.Popen(
        [sys.executable, '-B', OPS, 'promote', '--state', same,
         '--artifact', artifacts['r1'], '--expect', '0',
         '--request-id', 'one-id'],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        for _ in range(5)]
    results = [process.communicate() + (process.returncode,)
               for process in processes]
    check(all(result[2] == 0 for result in results),
          'same-id retries all succeed')
    results_by_generation = {json.loads(result[0])['generation']
                             for result in results}
    check(results_by_generation == {1}, 'same-id retries share one result')
    check(load_state(same)['generation'] == 1, 'same-id wrote one generation')
    evidence('concurrency', {'different_ids_accepted': len(successes),
                             'same_id_accepted': len(results),
                             'generation': 1})


def scenario_kill_boundaries(workdir, artifacts):
    # A: killed after the durable journal was written but before migration.
    state_a = os.path.join(workdir, 'state-kill-pending')
    check(json.loads(promote(state_a, artifacts['r1'], 0,
                             'k1-install').stdout)['generation'] == 1,
          'kill scenario install')
    ingest_with_artifact(artifacts['r1'],
                         os.path.join(state_a, 'ledger.sqlite'), 1, {
                             'batch_id': 'k-initial',
                             'rows': [{'id': 'k1', 'station': '江北',
                                       'day': '2026-10-31', 'amount': 12}]})
    run_paused(['promote', '--state', state_a, '--artifact',
                artifacts['r2'], '--expect', '1', '--request-id', 'k1-killed'],
               os.path.join(state_a, '.pause-after-pending'),
               {'POINTWISE_TEST_PAUSE_AT': 'after-pending',
                'POINTWISE_TEST_PAUSE_SECONDS': '30'})
    check(os.path.exists(os.path.join(state_a, 'pending.json')),
          'SIGKILL left the publication journal')
    check(load_state(state_a)['generation'] == 1, 'no state advance before commit')
    check(db_format(state_a) == 1, 'database still old format after SIGKILL')
    port = free_port()
    run = start_run(state_a, port, os.path.join(workdir, 'kill-a.log'))
    try:
        wait_health(port, 'r2', 2)
    finally:
        stop_run(run)
    check(load_state(state_a)['generation'] == 2, 'journal recovery commits')
    check(db_format(state_a) == 2, 'journal recovery database format')
    check(json.loads(ops('status', '--state', state_a).stdout)['requests'] > 0,
          'request record retained')
    evidence('kill_after_journal', {
        'pending_after_sigkill': True, 'recovered_generation': 2,
        'recovered_format': 2,
    })

    # B: killed after ledger.migrate() committed but before state.json.
    state_b = os.path.join(workdir, 'state-kill-migrate')
    promote(state_b, artifacts['r1'], 0, 'k2-install')
    ingest_with_artifact(artifacts['r1'],
                         os.path.join(state_b, 'ledger.sqlite'), 1, {
                             'batch_id': 'k2-initial',
                             'rows': [{'id': 'k2', 'station': '江北',
                                       'day': '2026-10-31', 'amount': 8}]})
    run_paused(['promote', '--state', state_b, '--artifact',
                artifacts['r2'], '--expect', '1', '--request-id', 'k2-killed'],
               os.path.join(state_b, '.pause-after-migrate'),
               {'POINTWISE_TEST_PAUSE_AT': 'after-migrate',
                'POINTWISE_TEST_PAUSE_SECONDS': '30'})
    check(load_state(state_b)['generation'] == 1,
          'state still previous generation after migrate kill')
    check(db_format(state_b) == 2,
          'database already converted at migrate kill')
    port = free_port()
    run = start_run(state_b, port, os.path.join(workdir, 'kill-b.log'))
    try:
        wait_health(port, 'r2', 2)
    finally:
        stop_run(run)
    check(load_state(state_b)['generation'] == 2,
          'start-up recovery committed the generation')
    check(db_format(state_b) == 2, 'start-up recovery format consistent')
    receipts = receivable_count(artifacts['r2'],
                                os.path.join(state_b, 'ledger.sqlite'), 2)
    check(set(name for name, _ in receipts) == {'k2-initial'},
          'start-up recovery preserved data and receipts')
    evidence('kill_after_migration', {
        'state_before_restart': 1, 'db_format_before_restart': 2,
        'recovered_generation': 2, 'data_preserved': True,
    })


def scenario_coordinator_kill(workdir, artifacts):
    state = os.path.join(workdir, 'state-coordinator')
    promote(state, artifacts['r1'], 0, 'c1-install')
    port = free_port()
    log = os.path.join(workdir, 'coordinator.log')
    run = start_run(state, port, log)
    try:
        wait_health(port, 'r1', 1)
        bulk = {'batch_id': 'c-bulk', 'rows': [
            {'id': 'c%05d' % index, 'station': '甲' if index % 2 else '乙',
             'day': '2026-11-03', 'amount': 2}
            for index in range(3000)]}
        check(request(port, 'POST', '/batches', bulk, timeout=30)[0] == 200,
              'coordinator bulk ingest')
        export_connection, rest = open_slow_export(port)
        os.kill(run.pid, signal.SIGKILL)
        run.wait()
        started = time.time()
        completed = promote(state, artifacts['r2'], 1, 'c1-switch')
        check(completed.returncode == 0, 'promote with no coordinator')
        check(time.time() - started < 10, 'promotion waited for export')
        failures = []
        for _ in range(8):
            failures.append(request(port, 'POST', '/batches',
                                    {'batch_id': 'c-noop', 'rows': []},
                                    timeout=1)[0])
            time.sleep(0.02)
        check(all(code in (None, 503) for code in failures),
              'stale worker never admitted a new write: %r' % (failures,))
        run2 = start_run(state, port, os.path.join(workdir,
                                                   'coordinator2.log'))
        try:
            status, body = retry_request(port, 'POST', '/batches', {
                'batch_id': 'c-after', 'rows': [
                    {'id': 'm4', 'station': '江北', 'day': '2026-11-01',
                     'amount': 30}]}, accept=(200,))
            check(status == 200, 'write after coordinator restart: %r'
                  % (body,))
            wait_health(port, 'r2', 2)
            exported = drain_export(export_connection, rest)
            check(len(exported) == 3000 and
                  not any(row['id'] == 'm4' for row in exported),
                  'export survived coordinator SIGKILL')
            evidence('coordinator_kill_export', {
                'switch_seconds': round(time.time() - started, 3),
                'old_export_rows': len(exported),
                'stale_write_codes': failures,
                'post_restart_write': 200,
            })
        finally:
            stop_run(run2)
    finally:
        if run.poll() is None:
            stop_run(run)


def scenario_damaged(workdir, artifacts):
    state = os.path.join(workdir, 'state-damaged')
    promote(state, artifacts['r1'], 0, 'd1-install')
    with open(artifacts['r2'], 'rb') as handle:
        raw = handle.read()
    damaged = os.path.join(workdir, 'damaged.pyz')
    with open(damaged, 'wb') as handle:
        handle.write(raw[:len(raw) // 2])
    before_generation = load_state(state)['generation']
    completed = promote(state, damaged, 1, 'd1-damaged')
    check(completed.returncode != 0, 'damaged artifact was rejected')
    check(load_state(state)['generation'] == before_generation,
          'damaged artifact did not change the online generation')
    evidence('damaged_artifact', {'rejected': True,
                                  'generation_unchanged': before_generation})


def scenario_launchagent(workdir, artifacts):
    state = os.path.join(workdir, '托管 状态')
    promote(state, artifacts['r1'], 0, 'l1-install')
    agents = os.path.join(workdir, 'LaunchAgents')
    environment = dict(os.environ)
    environment['POINTWISE_LAUNCH_AGENTS_DIR'] = agents
    installed = ops('agent-install', '--state', state, '--port', '18770',
                    env=environment)
    check(installed.returncode == 0, 'agent-install: %s' % installed.stderr)
    body = json.loads(installed.stdout)
    plist_path = body['plist']
    check(os.path.exists(plist_path), 'plist written')
    with open(plist_path, 'rb') as handle:
        plist = plistlib.load(handle)
    arguments = plist['ProgramArguments']
    check(os.path.isabs(arguments[0]) and os.path.exists(arguments[0]),
          'plist uses absolute interpreter')
    check(arguments[2] == os.path.join(state, 'bin', 'ops.py')
          and os.path.exists(arguments[2]),
          'plist bootstrap path is absolute installed entry')
    check(plist.get('KeepAlive') is True, 'KeepAlive present')
    check(plist.get('RunAtLoad') is True, 'RunAtLoad present')
    check(plist.get('StandardOutPath') and plist.get('StandardErrorPath'),
          'log paths present')
    check(plist.get('EnvironmentVariables', {}).get('PYTHONUNBUFFERED') == '1',
          'environment variables present')
    check(plist.get('WorkingDirectory') == state, 'state path embedded')
    uninstalled = ops('agent-uninstall', '--state', state, env=environment)
    check(uninstalled.returncode == 0, 'agent-uninstall')
    check(not os.path.exists(plist_path), 'plist removed on uninstall')
    check(os.path.exists(os.path.join(state, 'ledger.sqlite')),
          'uninstall kept the database')
    check(os.path.isdir(os.path.join(state, 'packages')) and
          os.listdir(os.path.join(state, 'packages')),
          'uninstall kept release packages')
    evidence('launchagent', {
        'plist': plist_path, 'paths_contain_spaces_and_chinese': True,
        'absolute_interpreter': True, 'keepalive': True,
        'uninstall_preserved_data': True,
    })


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--workdir', default=os.path.join(ROOT, 'var',
                                                          'evidence'))
    parser.add_argument('--port', type=int, default=None)
    args = parser.parse_args()
    workdir = os.path.abspath(args.workdir)
    shutil.rmtree(workdir, ignore_errors=True)
    os.makedirs(workdir, exist_ok=True)
    artifacts = {}
    artifacts['r1'] = build('r1', os.path.join(workdir, 'r1.pyz'))['artifact']
    artifacts['r2'] = build('r2', os.path.join(workdir, 'r2.pyz'))['artifact']
    port = args.port or free_port()

    business_state = os.path.join(workdir, 'state-business')
    log = os.path.join(workdir, 'business.log')
    scenario_business(business_state, port, artifacts, log)
    scenario_switch(business_state, port, artifacts, log)
    scenario_rollback(business_state, port, artifacts, log)
    scenario_concurrency(workdir, artifacts)
    scenario_keep = scenario_damaged(workdir, artifacts)
    scenario_kill_boundaries(workdir, artifacts)
    scenario_coordinator_kill(workdir, artifacts)
    scenario_launchagent(workdir, artifacts)
    check(len(RECEIVED) >= 8, 'all evidence groups produced')
    print('EVIDENCE_OK')


if __name__ == '__main__':
    try:
        main()
    except AssertionError as exc:
        traceback.print_exc()
        print('EVIDENCE_FAILED: %s' % exc)
        raise SystemExit(1)
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
        traceback.print_exc()
        print('EVIDENCE_ERROR: %r' % exc)
        raise SystemExit(1)
