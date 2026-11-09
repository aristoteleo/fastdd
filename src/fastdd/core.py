"""File processing, resumable state, and thread-safe statistics."""

import json
import os
import queue
import sqlite3
import stat
import threading
import time
from collections import deque
from . import __version__ as VERSION

def human_bytes(n):
    n = float(max(0, n))
    units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']
    for unit in units:
        if n < 1024 or unit == units[-1]:
            return f'{n:.2f} {unit}'
        n /= 1024

def human_time(sec):
    if sec is None:
        return '--'
    if sec < 0 or sec == float('inf'):
        return '--'
    sec = int(sec)
    days, sec = divmod(sec, 86400)
    hours, sec = divmod(sec, 3600)
    minutes, seconds = divmod(sec, 60)
    if days:
        return f'{days}d {hours:02d}:{minutes:02d}:{seconds:02d}'
    return f'{hours:02d}:{minutes:02d}:{seconds:02d}'

def atomic_json(path, data):
    tmp = path + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(data, f, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)

def remove_nested_roots(paths):
    """
    如果 glob 同时匹配到父目录和子目录，
    避免重复扫描。
    """
    paths = sorted(set((os.path.abspath(p) for p in paths)))
    kept = []
    for path in paths:
        nested = False
        for previous in kept:
            if not os.path.isdir(previous):
                continue
            try:
                if os.path.commonpath([path, previous]) == previous:
                    nested = True
                    break
            except ValueError:
                pass
        if not nested:
            kept.append(path)
    return kept

class Stats:

    def __init__(self, mode, workers, retention_policy=None):
        self.lock = threading.Lock()
        self.mode = mode
        self.workers = workers
        self.started = time.time()
        self.started_monotonic = time.monotonic()
        self.finished = False
        self.interrupted = False
        self.processed_bytes = 0
        self.scan_done = False
        self.discovered_files = 0
        self.discovered_bytes = 0
        self.queued = 0
        self.done_files = 0
        self.done_bytes = 0
        self.skipped_files = 0
        self.skipped_bytes = 0
        self.retention_policy = retention_policy
        self.time_skipped_files = 0
        self.time_skipped_bytes = 0
        self.recent_files = deque(maxlen=5)
        self.failed_files = 0
        self.failed_bytes = 0
        self.active = {}
        self.errors = deque(maxlen=20)
        self.rate_samples = deque([(self.started_monotonic, 0)])

    def snapshot(self):
        with self.lock:
            now = time.time()
            tick = time.monotonic()
            self.rate_samples.append((tick, self.processed_bytes))
            while len(self.rate_samples) > 2 and tick - self.rate_samples[1][0] > 10:
                self.rate_samples.popleft()
            span = max(0.001, tick - self.rate_samples[0][0])
            rate = (self.processed_bytes - self.rate_samples[0][1]) / span
            active_bytes = sum(info.get('processed', 0) for info in self.active.values())
            remaining = max(0, self.discovered_bytes - self.done_bytes - self.skipped_bytes - self.failed_bytes - active_bytes)
            if self.scan_done and rate > 0:
                eta = remaining / rate
            else:
                eta = None
            total_finished = self.done_bytes + self.skipped_bytes + self.failed_bytes + active_bytes
            if self.scan_done and self.discovered_bytes:
                percent = min(100.0, 100.0 * total_finished / self.discovered_bytes)
            elif self.scan_done:
                percent = 100.0 if not self.active and not self.queued else 0.0
            else:
                percent = None
            return {
                'version': VERSION, 'pid': os.getpid(), 'mode': self.mode,
                'workers': self.workers, 'started': self.started, 'updated': now,
                'scan_done': self.scan_done, 'discovered_files': self.discovered_files,
                'discovered_bytes': self.discovered_bytes, 'queued': self.queued,
                'done_files': self.done_files, 'done_bytes': self.done_bytes,
                'skipped_files': self.skipped_files, 'skipped_bytes': self.skipped_bytes,
                'failed_files': self.failed_files, 'failed_bytes': self.failed_bytes,
                'active': {key: dict(value) for key, value in self.active.items()},
                'errors': list(self.errors), 'rate_bps': rate, 'eta_seconds': eta,
                'percent': percent, 'elapsed_seconds': tick - self.started_monotonic,
                'finished': self.finished, 'interrupted': self.interrupted,
                'processed_bytes': self.processed_bytes,
                'retention_policy': ({
                    'basis': 'mtime',
                    'retention_days': self.retention_policy.retention_days,
                    'skip_if_remaining_days': self.retention_policy.skip_if_remaining_days,
                } if self.retention_policy else None),
                'time_skipped_files': self.time_skipped_files,
                'time_skipped_bytes': self.time_skipped_bytes,
                'recent_files': list(self.recent_files),
            }

def init_db(path):
    conn = sqlite3.connect(path, check_same_thread=False, timeout=60)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    # Separate schema: old records do not identify the mode and must be rerun.
    conn.execute('''CREATE TABLE IF NOT EXISTS completed_v2 (
        path TEXT NOT NULL, mode TEXT NOT NULL, size INTEGER NOT NULL,
        mtime_ns INTEGER NOT NULL, completed_at REAL NOT NULL,
        PRIMARY KEY (path, mode))''')
    conn.commit()
    return conn

def load_completed(conn, mode):
    completed = {}
    cursor = conn.execute('SELECT path, size, mtime_ns FROM completed_v2 WHERE mode = ?', (mode,))
    for path, size, mtime_ns in cursor:
        completed[path] = (int(size), int(mtime_ns))
    return completed

def write_all(fd, data, offset):
    position = 0
    total = len(data)
    while position < total:
        written = os.pwrite(fd, data[position:], offset + position)
        if written <= 0:
            raise OSError('pwrite returned 0')
        position += written

def process_file(path, mode, chunk_size, do_fsync, stop_event=None, progress=None, refresh_empty=False):
    if mode not in ('read', 'rewrite') or chunk_size < 1:
        raise ValueError('invalid mode or chunk size')
    st = os.stat(path, follow_symlinks=False)
    if not stat.S_ISREG(st.st_mode):
        raise ValueError('input must be a regular file, not a symlink')
    size = st.st_size
    if mode == 'read':
        flags = os.O_RDONLY
    else:
        flags = os.O_RDWR
    fd = os.open(path, flags | getattr(os, 'O_NOFOLLOW', 0) | os.O_NONBLOCK)
    try:
        opened = os.fstat(fd)
        if (opened.st_dev, opened.st_ino, opened.st_size) != (st.st_dev, st.st_ino, size):
            raise RuntimeError('file changed before opening')
        if hasattr(os, 'posix_fadvise'):
            try:
                os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_SEQUENTIAL)
            except Exception:
                pass
        offset = 0
        while offset < size:
            if stop_event is not None and stop_event.is_set():
                raise InterruptedError('processing stopped; file will restart on resume')
            request_size = min(chunk_size, size - offset)
            data = os.pread(fd, request_size, offset)
            if not data:
                raise IOError(f'unexpected EOF at offset {offset}')
            if mode == 'rewrite':
                write_all(fd, data, offset)
            offset += len(data)
            if progress is not None:
                progress(len(data))
        # Empty files have no blocks to rewrite. Refresh mtime only when the
        # user explicitly enabled the mtime retention policy.
        if mode == 'rewrite' and size == 0 and refresh_empty:
            os.utime(fd, ns=(opened.st_atime_ns, time.time_ns()))
        if mode == 'rewrite' and do_fsync:
            os.fsync(fd)
        final_stat = os.fstat(fd)
        if final_stat.st_size != size:
            raise RuntimeError(f'file size changed during processing: {size} -> {final_stat.st_size}')
        if mode == 'read' and final_stat.st_mtime_ns != st.st_mtime_ns:
            raise RuntimeError('file modified during reading')
        return (size, final_stat.st_mtime_ns)
    finally:
        os.close(fd)

def worker_loop(worker_id, work_queue, stats, stop_event, mode, chunk_size, do_fsync, db_conn, db_lock, completed_cache):
    while not stop_event.is_set():
        try:
            item = work_queue.get(timeout=0.5)
        except queue.Empty:
            continue
        if item is None:
            work_queue.task_done()
            return
        path, expected_size = item
        with stats.lock:
            stats.queued = work_queue.qsize()
            stats.active[str(worker_id)] = {'path': path, 'size': expected_size, 'started': time.time()}
        try:
            if stats.retention_policy:
                # Recheck immediately before I/O: another process may have
                # refreshed the file since it was queued.
                timing = stats.retention_policy.describe(
                    os.stat(path, follow_symlinks=False).st_mtime, time.time())
                with stats.lock:
                    stats.active[str(worker_id)].update(timing)
                    if timing['skip']:
                        stats.skipped_files += 1
                        stats.skipped_bytes += expected_size
                        stats.time_skipped_files += 1
                        stats.time_skipped_bytes += expected_size
                        stats.recent_files.append(dict(timing, path=path, action='SKIPPED'))
                if timing['skip']:
                    continue

            def progress(amount):
                with stats.lock:
                    stats.processed_bytes += amount
                    info = stats.active[str(worker_id)]
                    info['processed'] = info.get('processed', 0) + amount

            size, mtime_ns = process_file(path, mode, chunk_size, do_fsync, stop_event, progress,
                                         refresh_empty=bool(stats.retention_policy))
            with db_lock:
                db_conn.execute('''INSERT OR REPLACE INTO completed_v2
                    (path, mode, size, mtime_ns, completed_at) VALUES (?, ?, ?, ?, ?)''',
                    (path, mode, size, mtime_ns, time.time()))
                db_conn.commit()
                completed_cache[path] = (size, mtime_ns)
            with stats.lock:
                stats.done_files += 1
                stats.done_bytes += size
                stats.active[str(worker_id)]['processed'] = 0
                if stats.retention_policy:
                    timing = stats.retention_policy.describe(mtime_ns / 1_000_000_000, time.time())
                    stats.recent_files.append(dict(timing, path=path, action='DONE'))
        except InterruptedError:
            pass
        except Exception as error:
            with stats.lock:
                stats.failed_files += 1
                stats.failed_bytes += expected_size
                stats.active[str(worker_id)]['processed'] = 0
                stats.errors.append({'path': path, 'error': repr(error), 'time': time.time()})
        finally:
            with stats.lock:
                stats.active.pop(str(worker_id), None)
                stats.queued = work_queue.qsize()
            work_queue.task_done()

def iter_files(roots, excluded=(), onerror=None):
    seen = set()
    excluded = {os.path.realpath(path) for path in excluded}
    for root in roots:
        if os.path.islink(root):
            continue
        if os.path.isfile(root):
            path = os.path.abspath(root)
            if path not in seen and os.path.realpath(path) not in excluded:
                seen.add(path)
                yield path
            continue
        if not os.path.isdir(root):
            continue
        for directory, directory_names, filenames in os.walk(root, followlinks=False, onerror=onerror):
            for filename in filenames:
                path = os.path.join(directory, filename)
                if path in seen or os.path.realpath(path) in excluded:
                    continue
                seen.add(path)
                try:
                    if os.path.isfile(path) and (not os.path.islink(path)):
                        yield path
                except OSError:
                    continue
