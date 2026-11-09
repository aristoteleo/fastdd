"""Run a real, disposable mtime-filter demonstration (48 MiB of test data)."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time


def main():
    executable = shutil.which('fastdd')
    if executable is None:
        raise SystemExit('Install fastdd first: pip install fastdd')
    with tempfile.TemporaryDirectory(prefix='fastdd-demo-') as directory:
        root = Path(directory)
        data = root / 'data'
        data.mkdir()
        now = time.time()
        block = bytes(range(256)) * 4096
        originals = {}
        for name, age in (
            ('archive-75d.bin', 75), ('checkpoint-65d.bin', 65),
            ('counts-80d.bin', 80), ('matrix-70d.bin', 70),
            ('recent-10d.bin', 10), ('recent-20d.bin', 20),
        ):
            path = data / name
            with path.open('wb') as handle:
                for _ in range(8):
                    handle.write(block)
            os.utime(path, (now, now - age * 86400))
            originals[path] = hashlib.sha256(path.read_bytes()).digest()

        # Short relative paths keep the terminal output easy to read.
        args = ['run', 'data', '--mode', 'rewrite', '--workers', '2',
                '--chunk-mb', '1', '--fsync', '--retention-days', '90',
                '--skip-if-remaining-days', '30', '--state', 'state',
                '--color', 'always']
        subprocess.run([executable, *args], cwd=root, check=True)
        status = json.loads((root / 'state.status.json').read_text())
        if (status['done_files'], status['time_skipped_files'], status['failed_files']) != (4, 2, 0):
            raise RuntimeError('Unexpected demo processing counts')
        for path, digest in originals.items():
            if hashlib.sha256(path.read_bytes()).digest() != digest:
                raise RuntimeError(f'Content changed: {path.name}')
        print('  Demo verified: 4 rewritten, 2 skipped; all 6 SHA-256 checksums unchanged.', flush=True)
        print('  Local temporary test files only. Throughput is not a server benchmark.', flush=True)


if __name__ == '__main__':
    main()
