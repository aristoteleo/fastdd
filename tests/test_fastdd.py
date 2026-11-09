import hashlib
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from fastdd.core import process_file, write_all
from fastdd.terminal import use_color


class FastDDTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.data = self.root / 'data'
        self.data.mkdir()
        self.file = self.data / 'payload.bin'
        self.payload = bytes(range(256)) * 8193
        self.file.write_bytes(self.payload)
        (self.data / 'empty').touch()
        self.state = self.data / '.state'

    def cli(self, *args, code=0):
        result = subprocess.run([sys.executable, '-m', 'fastdd', *map(str, args)],
                                capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, code, result.stdout + result.stderr)
        return result

    def run_files(self, *args):
        return self.cli('run', self.data, '--state', self.state, '--workers', '2', '--queue-size', '1', *args)

    def status(self):
        return json.loads(Path(str(self.state) + '.status.json').read_text())

    def test_read_resume_rewrite_and_state_exclusion(self):
        before = hashlib.sha256(self.file.read_bytes()).digest()
        self.run_files()
        self.assertEqual(self.status()['done_files'], 2)
        self.assertEqual(self.status()['discovered_files'], 2)
        self.run_files()
        self.assertEqual(self.status()['skipped_files'], 2)
        self.run_files('--mode', 'rewrite', '--fsync')
        self.assertEqual(self.status()['done_files'], 2)
        self.assertEqual(hashlib.sha256(self.file.read_bytes()).digest(), before)
        self.run_files('--mode', 'rewrite')
        self.assertEqual(self.status()['skipped_files'], 2)
        self.run_files('--mode', 'rewrite', '--no-resume')
        self.assertEqual(self.status()['done_files'], 2)

    def test_changed_file_reprocessed(self):
        self.run_files()
        self.file.write_bytes(b'changed')
        self.run_files()
        self.assertEqual(self.status()['done_files'], 1)
        self.assertEqual(self.status()['skipped_files'], 1)

    def test_color_and_watch_finished(self):
        result = self.run_files('--color', 'always')
        self.assertIn('\033[', result.stdout)
        result = self.cli('watch', '--state', self.state, '--color', 'never')
        self.assertIn('COMPLETE', result.stdout)
        self.assertNotIn('\033[', result.stdout)
        result = self.cli('--color', 'always', 'watch', '--state', self.state, '--once')
        self.assertIn('\033[', result.stdout)
        result = self.run_files()
        self.assertNotIn('\033[', result.stdout)

    def test_no_color_environment(self):
        terminal = io.StringIO()
        terminal.isatty = lambda: True
        with patch.dict(os.environ, {'NO_COLOR': ''}):
            self.assertFalse(use_color('auto', terminal))
            self.assertTrue(use_color('always', terminal))

    def test_partial_writes_and_progress(self):
        written = bytearray()
        def short_write(fd, data, offset):
            self.assertEqual(offset, len(written))
            written.extend(data[:3])
            return len(data[:3])
        with patch('fastdd.core.os.pwrite', side_effect=short_write):
            write_all(123, b'abcdefgh', 0)
        self.assertEqual(written, b'abcdefgh')
        chunks = []
        process_file(str(self.file), 'read', 100_000, False, progress=chunks.append)
        self.assertGreater(len(chunks), 1)
        self.assertEqual(sum(chunks), len(self.payload))

    def test_symlinks_excluded(self):
        (self.data / 'alias').symlink_to(self.file)
        self.run_files()
        self.assertEqual(self.status()['discovered_files'], 2)
        with self.assertRaises(ValueError):
            process_file(str(self.data / 'alias'), 'rewrite', 1024, False)

    def test_invalid_arguments(self):
        self.cli('run', self.data, '--state', self.state, '--workers', '0', code=2)
        for value in ('0', '-1', 'nan', 'inf'):
            self.cli('watch', '--state', self.state, '--interval', value, code=2)
        self.cli('watch', '--state', self.state, '--once', code=2)

    def test_interrupt_with_full_queue(self):
        # Slow only the test child's pread so a signal reliably lands during I/O.
        script = '''
import os, sys, time
from fastdd.cli import main
original = os.pread
def slow_read(*args):
    time.sleep(0.2)
    return original(*args)
os.pread = slow_read
raise SystemExit(main(sys.argv[1:]))
'''
        for index in range(5):
            (self.data / f'extra{index}').write_bytes(self.payload)
        child = subprocess.Popen([sys.executable, '-c', script, 'run', str(self.data),
                                  '--state', str(self.state), '--workers', '1',
                                  '--queue-size', '1', '--chunk-mb', '1'],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            deadline = time.monotonic() + 10
            while not Path(str(self.state) + '.status.json').exists():
                if time.monotonic() > deadline:
                    self.fail('child did not start')
                time.sleep(0.01)
            time.sleep(0.15)
            child.send_signal(signal.SIGTERM)
            out, err = child.communicate(timeout=10)
            self.assertEqual(child.returncode, 130, out + err)
            self.assertTrue(self.status()['finished'])
            self.assertTrue(self.status()['interrupted'])
            self.assertFalse(self.status()['active'])
            self.cli('watch', '--state', self.state)
            self.run_files()
            self.assertEqual(self.status()['failed_files'], 0)
            self.assertEqual(self.status()['done_files'] + self.status()['skipped_files'], 7)
        finally:
            if child.poll() is None:
                child.kill()
                child.communicate()


if __name__ == '__main__':
    unittest.main()
