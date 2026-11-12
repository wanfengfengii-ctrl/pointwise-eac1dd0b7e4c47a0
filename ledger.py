import contextlib
import hashlib
import json
import sqlite3

class LedgerError(Exception):
    pass

def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)

def connect(path):
    c = sqlite3.connect(str(path), timeout=10, isolation_level=None)
    c.execute('PRAGMA journal_mode=WAL')
    c.execute('PRAGMA foreign_keys=ON')
    return c

@contextlib.contextmanager
def transaction(c, write=False):
    c.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
    try:
        yield
        c.execute('COMMIT')
    except BaseException:
        c.execute('ROLLBACK')
        raise

def schema(c):
    row = c.execute('SELECT version FROM format').fetchone()
    if row is None:
        raise LedgerError('NO_FORMAT')
    return row[0]

def initialize(c, version):
    if version not in (1, 2):
        raise LedgerError('BAD_FORMAT')
    with transaction(c, True):
        c.execute('CREATE TABLE IF NOT EXISTS format(version INTEGER NOT NULL)')
        if c.execute('SELECT 1 FROM format').fetchone():
            return
        c.execute('INSERT INTO format VALUES (?)', (version,))
        c.execute('CREATE TABLE receipts(id TEXT PRIMARY KEY, digest TEXT NOT NULL)')
        if version == 1:
            c.execute('CREATE TABLE measurements(id TEXT PRIMARY KEY, station TEXT NOT NULL, day TEXT NOT NULL, amount INTEGER NOT NULL)')
        else:
            c.execute('CREATE TABLE stations(n INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL)')
            c.execute('CREATE TABLE readings(id TEXT PRIMARY KEY, station_n INTEGER NOT NULL REFERENCES stations(n), day TEXT NOT NULL, amount INTEGER NOT NULL)')

def validate(batch):
    if not isinstance(batch, dict) or set(batch) != {'batch_id', 'rows'}:
        raise LedgerError('BAD_BATCH')
    if not isinstance(batch['batch_id'], str) or not batch['batch_id']:
        raise LedgerError('BAD_BATCH_ID')
    if not isinstance(batch['rows'], list):
        raise LedgerError('BAD_ROWS')
    seen = set()
    for row in batch['rows']:
        if not isinstance(row, dict) or set(row) != {'id', 'station', 'day', 'amount'}:
            raise LedgerError('BAD_ROW')
        if any(not isinstance(row[k], str) or not row[k] for k in ('id', 'station', 'day')):
            raise LedgerError('BAD_ROW')
        if type(row['amount']) is not int or abs(row['amount']) > 1000000000:
            raise LedgerError('BAD_AMOUNT')
        if row['id'] in seen:
            raise LedgerError('DUPLICATE_ROW')
        seen.add(row['id'])
    return hashlib.sha256(canonical(batch).encode('utf-8')).hexdigest()

def ingest(c, version, batch):
    digest = validate(batch)
    with transaction(c, True):
        if schema(c) != version:
            raise LedgerError('FORMAT_MISMATCH')
        previous = c.execute('SELECT digest FROM receipts WHERE id=?', (batch['batch_id'],)).fetchone()
        if previous:
            if previous[0] != digest:
                raise LedgerError('BATCH_CONFLICT')
            return {'batch_id': batch['batch_id'], 'rows': len(batch['rows'])}
        for row in batch['rows']:
            if version == 1:
                c.execute('INSERT INTO measurements VALUES (?,?,?,?)', (row['id'], row['station'], row['day'], row['amount']))
            else:
                c.execute('INSERT OR IGNORE INTO stations(name) VALUES (?)', (row['station'],))
                n = c.execute('SELECT n FROM stations WHERE name=?', (row['station'],)).fetchone()[0]
                c.execute('INSERT INTO readings VALUES (?,?,?,?)', (row['id'], n, row['day'], row['amount']))
        c.execute('INSERT INTO receipts VALUES (?,?)', (batch['batch_id'], digest))
    return {'batch_id': batch['batch_id'], 'rows': len(batch['rows'])}

def query(version):
    if version == 1:
        return 'SELECT id,station,day,amount FROM measurements ORDER BY id'
    return 'SELECT r.id,s.name,r.day,r.amount FROM readings r JOIN stations s ON s.n=r.station_n ORDER BY r.id'

def export(c, version):
    # The transaction remains pinned until the consumer finishes or closes the generator.
    with transaction(c):
        if schema(c) != version:
            raise LedgerError('FORMAT_MISMATCH')
        for row in c.execute(query(version)):
            yield dict(zip(('id', 'station', 'day', 'amount'), row))

def snapshot(c, version):
    groups = {}
    count = 0
    for row in export(c, version):
        key = (row['day'], row['station'])
        old = groups.get(key, (0, 0))
        groups[key] = (old[0] + row['amount'], old[1] + 1)
        count += 1
    return {'count': count, 'daily': [
        {'day': key[0], 'station': key[1], 'amount': value[0], 'count': value[1]}
        for key, value in sorted(groups.items())
    ]}

def migrate(c, target):
    if target not in (1, 2):
        raise LedgerError('BAD_FORMAT')
    # This adapter deliberately owns its transaction; callers cannot compose another commit into it.
    with transaction(c, True):
        old = schema(c)
        if old == target:
            return
        if target == 2:
            c.execute('CREATE TABLE stations(n INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL)')
            c.execute('INSERT INTO stations(name) SELECT DISTINCT station FROM measurements ORDER BY station')
            c.execute('CREATE TABLE readings(id TEXT PRIMARY KEY, station_n INTEGER NOT NULL REFERENCES stations(n), day TEXT NOT NULL, amount INTEGER NOT NULL)')
            c.execute('INSERT INTO readings SELECT m.id,s.n,m.day,m.amount FROM measurements m JOIN stations s ON s.name=m.station')
            c.execute('DROP TABLE measurements')
        else:
            c.execute('CREATE TABLE measurements(id TEXT PRIMARY KEY, station TEXT NOT NULL, day TEXT NOT NULL, amount INTEGER NOT NULL)')
            c.execute('INSERT INTO measurements SELECT r.id,s.name,r.day,r.amount FROM readings r JOIN stations s ON s.n=r.station_n')
            c.execute('DROP TABLE readings')
            c.execute('DROP TABLE stations')
        c.execute('UPDATE format SET version=?', (target,))

def receipts(c):
    return list(c.execute('SELECT id,digest FROM receipts ORDER BY id'))
