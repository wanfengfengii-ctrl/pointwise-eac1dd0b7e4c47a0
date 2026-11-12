INITIAL = {
    'batch_id': 'initial',
    'rows': [
        {'id': 'm1', 'station': '江北', 'day': '2026-10-31', 'amount': 120},
        {'id': 'm2', 'station': '江北', 'day': '2026-10-31', 'amount': -20},
        {'id': 'm3', 'station': '南站', 'day': '2026-11-01', 'amount': 70}
    ]
}
ARRIVAL = {
    'batch_id': 'during-r2',
    'rows': [
        {'id': 'm4', 'station': '江北', 'day': '2026-11-01', 'amount': 30}
    ]
}
INITIAL_EXPECTED = {
    'count': 3,
    'daily': [
        {'day': '2026-10-31', 'station': '江北', 'amount': 100, 'count': 2},
        {'day': '2026-11-01', 'station': '南站', 'amount': 70, 'count': 1}
    ]
}
FINAL_EXPECTED = {
    'count': 4,
    'daily': [
        {'day': '2026-10-31', 'station': '江北', 'amount': 100, 'count': 2},
        {'day': '2026-11-01', 'station': '南站', 'amount': 70, 'count': 1},
        {'day': '2026-11-01', 'station': '江北', 'amount': 30, 'count': 1}
    ]
}
