import copy
import json
import sqlite3
import sys
import ledger
import samples

def check():
    c = ledger.connect(':memory:')
    ledger.initialize(c, 1)
    receipt = ledger.ingest(c, 1, samples.INITIAL)
    assert ledger.ingest(c, 1, samples.INITIAL) == receipt
    assert ledger.snapshot(c, 1) == samples.INITIAL_EXPECTED
    original = ledger.receipts(c)
    ledger.migrate(c, 2)
    assert ledger.snapshot(c, 2) == samples.INITIAL_EXPECTED
    assert ledger.receipts(c) == original
    try:
        ledger.ingest(c, 1, samples.ARRIVAL)
    except ledger.LedgerError as exc:
        assert str(exc) == 'FORMAT_MISMATCH'
    else:
        raise AssertionError('old writer entered new schema')
    ledger.ingest(c, 2, samples.ARRIVAL)
    assert ledger.snapshot(c, 2) == samples.FINAL_EXPECTED
    bad = copy.deepcopy(samples.INITIAL)
    bad['rows'][0]['amount'] += 1
    try:
        ledger.ingest(c, 2, bad)
    except ledger.LedgerError as exc:
        assert str(exc) == 'BATCH_CONFLICT'
    else:
        raise AssertionError('conflicting replay accepted')
    before = ledger.receipts(c)
    bad = {'batch_id': 'atomic', 'rows': [
        {'id': 'transient', 'station': '西站', 'day': '2026-11-01', 'amount': 11},
        {'id': 'm1', 'station': '西站', 'day': '2026-11-01', 'amount': 12}
    ]}
    try:
        ledger.ingest(c, 2, bad)
    except sqlite3.IntegrityError:
        pass
    else:
        raise AssertionError('duplicate measurement accepted')
    assert ledger.snapshot(c, 2) == samples.FINAL_EXPECTED
    assert ledger.receipts(c) == before
    ledger.migrate(c, 1)
    assert ledger.snapshot(c, 1) == samples.FINAL_EXPECTED
    assert ledger.receipts(c) == before
    assert ledger.ingest(c, 1, samples.ARRIVAL) == {'batch_id': 'during-r2', 'rows': 1}
    assert ledger.snapshot(c, 1) == samples.FINAL_EXPECTED
    assert list(ledger.export(c, 1)) == sorted(samples.INITIAL['rows'] + samples.ARRIVAL['rows'], key=lambda r: r['id'])
    c.close()
    print('BUSINESS_REGRESSION_OK')

def demo():
    c = ledger.connect(':memory:')
    ledger.initialize(c, 1)
    ledger.ingest(c, 1, samples.INITIAL)
    print(json.dumps({'phase': 'r1', 'snapshot': ledger.snapshot(c, 1)}, ensure_ascii=False))
    ledger.migrate(c, 2)
    ledger.ingest(c, 2, samples.ARRIVAL)
    print(json.dumps({'phase': 'r2-arrival', 'snapshot': ledger.snapshot(c, 2)}, ensure_ascii=False))
    ledger.migrate(c, 1)
    print(json.dumps({'phase': 'r1-after-reverse-conversion', 'snapshot': ledger.snapshot(c, 1), 'receipts': ledger.receipts(c)}, ensure_ascii=False))
    c.close()

if __name__ == '__main__':
    mode = sys.argv[1] if len(sys.argv) > 1 else 'normal'
    if mode == 'normal':
        check()
    elif mode == 'demo':
        demo()
    else:
        raise SystemExit('use normal or demo')
