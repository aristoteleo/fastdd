import json
import os
from pathlib import Path
import queue
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from fastdd.retention import DAY_SECONDS, RetentionPolicy
from fastdd.core import Stats, init_db, worker_loop


class RetentionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.data = self.root / 'data'
        self.data.mkdir()
        self.state = self.root / 'state'
        self.now = time.time()

    def make_file(self, name, age, content=b'keep these bytes\n'):
        path = self.data / name
        path.write_bytes(content)
        # Deliberately recent atime, old mtime: only mtime should matter.
        os.utime(path, (self.now, self.now - age * DAY_SECONDS))
        return path

    def run_cli(self, *args, code=0):
        result = subprocess.run([
            sys.executable, '-m', 'fastdd', 'run', str(self.data),
            '--state', str(self.state), '--workers', '1', '--color', 'never',
            *args,
        ], capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, code, result.stdout + result.stderr)
        return result

    def status(self):
        return json.loads(Path(str(self.state) + '.status.json').read_text())

    def test_strict_threshold_and_expired_files(self):
        policy = RetentionPolicy(90, 30)
        now = 2_000_000_000
        for age, should_skip in ((59.999, True), (60, False), (60.001, False), (90, False), (95, False), (-1, True)):
            with self.subTest(age=age):
                result = policy.describe(now - age * DAY_SECONDS, now)
                self.assertEqual(result['skip'], should_skip)
                self.assertAlmostEqual(result['remaining_days'], 90 - age)

    def test_rewrite_filters_by_mtime_and_refreshes_empty_file(self):
        recent = self.make_file('recent', 20)
        due = self.make_file('due', 65)
        overdue = self.make_file('overdue', 95)
        empty = self.make_file('empty', 70, b'')
        original_recent_mtime = recent.stat().st_mtime_ns
        original_content = {p: p.read_bytes() for p in (recent, due, overdue, empty)}
        output = self.run_cli('--mode', 'rewrite', '--retention-days', '90',
                              '--skip-if-remaining-days', '30', '--fsync')
        status = self.status()
        self.assertEqual(status['done_files'], 3)
        self.assertEqual(status['time_skipped_files'], 1)
        self.assertEqual(status['skipped_files'], 1)
        self.assertEqual(status['failed_files'], 0)
        self.assertEqual(status['percent'], 100)
        self.assertEqual(status['retention_policy']['basis'], 'mtime')
        self.assertEqual(recent.stat().st_mtime_ns, original_recent_mtime)
        for path in (due, overdue, empty):
            self.assertGreaterEqual(path.stat().st_mtime, self.now)
        for path, content in original_content.items():
            self.assertEqual(path.read_bytes(), content)
        self.assertIn('mtime', output.stdout)
        self.assertIn('est. cleanup', output.stdout)
        self.assertIn('days left', output.stdout)
        self.assertIn('Started', output.stdout)
        # --no-resume does not override the explicit time filter.
        self.run_cli('--mode', 'rewrite', '--skip-if-remaining-days', '30', '--no-resume')
        self.assertEqual(self.status()['done_files'], 0)
        self.assertEqual(self.status()['time_skipped_files'], 4)

    def test_old_resume_record_does_not_hide_due_file(self):
        path = self.make_file('due', 70)
        old_mtime = path.stat().st_mtime_ns
        self.run_cli('--mode', 'read')
        self.assertEqual(self.status()['done_files'], 1)
        output = self.run_cli('--mode', 'read', '--skip-if-remaining-days', '30')
        self.assertEqual(self.status()['done_files'], 1)
        self.assertEqual(self.status()['skipped_files'], 0)
        self.assertEqual(path.stat().st_mtime_ns, old_mtime)
        self.assertIn('does not refresh mtime', output.stderr)

    def test_custom_retention(self):
        self.make_file('due', 12)
        self.make_file('recent', 2)
        self.run_cli('--mode', 'rewrite', '--retention-days', '14', '--skip-if-remaining-days', '3')
        self.assertEqual(self.status()['done_files'], 1)
        self.assertEqual(self.status()['time_skipped_files'], 1)

    def test_worker_rechecks_a_refreshed_queued_file(self):
        path = self.make_file('queued', 70)
        work = queue.Queue()
        work.put((str(path), path.stat().st_size))
        work.put(None)
        os.utime(path, (self.now, self.now))
        refreshed_mtime = path.stat().st_mtime_ns
        stats = Stats('rewrite', 1, RetentionPolicy())
        db = init_db(str(self.state) + '.sqlite')
        try:
            worker_loop(1, work, stats, threading.Event(), 'rewrite', 1024, False,
                        db, threading.Lock(), {})
            self.assertEqual(stats.time_skipped_files, 1)
            self.assertEqual(stats.done_files, 0)
            self.assertEqual(work.unfinished_tasks, 0)
            self.assertFalse(stats.active)
            self.assertEqual(path.stat().st_mtime_ns, refreshed_mtime)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM completed_v2').fetchone()[0], 0)
        finally:
            db.close()

    def test_bad_thresholds(self):
        for value in ('nan', 'inf', '-1', '91'):
            self.run_cli('--skip-if-remaining-days', value, code=2)
        for value in ('nan', 'inf', '0', '-1'):
            self.run_cli('--retention-days', value, code=2)


if __name__ == '__main__':
    unittest.main()
