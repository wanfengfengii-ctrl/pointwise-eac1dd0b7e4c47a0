"""Entry point of the immutable pointwise release zipapp."""

import json
import os
import sys
import zipfile


def _embedded_manifest():
    path = os.path.abspath(sys.argv[0])
    with zipfile.ZipFile(path) as archive:
        return json.loads(archive.read('MANIFEST.json').decode('utf-8'))


def main():
    args = sys.argv[1:]
    if args[:1] == ['--worker']:
        from runtime.worker import main as worker_main
        return worker_main(args[1:])
    if args[:1] == ['--manifest']:
        print(json.dumps(_embedded_manifest(), ensure_ascii=False,
                         sort_keys=True, separators=(',', ':')))
        return 0
    if args[:1] == ['--checks']:
        import checks
        mode = args[1] if len(args) > 1 else 'normal'
        if mode == 'normal':
            checks.check()
        elif mode == 'demo':
            checks.demo()
        else:
            raise SystemExit('use normal or demo')
        return 0
    print(json.dumps({'release': _embedded_manifest().get('release'),
                      'format': _embedded_manifest().get('format'),
                      'usage': ['--worker', '--manifest', '--checks']},
                     ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
