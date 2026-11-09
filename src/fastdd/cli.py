"""The fastdd console entry point."""

import argparse
import glob
import json
import math
import os
import queue
import signal
import sys
import threading
import time
from contextlib import contextmanager

from . import __version__
from .core import (Stats, atomic_json, human_bytes, init_db, iter_files,
                   load_completed, remove_nested_roots, worker_loop)
from .terminal import paint, render_status, safe_text, use_color
from .retention import RetentionPolicy


@contextmanager
def state_lock(path):
    import fcntl

    with open(path, 'a') as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('another fastdd run is using this state prefix') from None
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def display(status, color, clear=False):
    if clear and sys.stdout.isatty() and os.environ.get('TERM') != 'dumb':
        sys.stdout.write('\033[2J\033[H')
    print(render_status(status, color), flush=True)


def status_writer(stats, path, done, color):
    last_log = 0
    while not done.is_set():
        status = stats.snapshot()
        try:
            atomic_json(path, status)
        except OSError as error:
            print(f'fastdd: cannot write status: {error}', file=sys.stderr, flush=True)
            return
        if sys.stdout.isatty():
            display(status, color, clear=True)
        elif time.monotonic() - last_log >= 30:
            print(paint(f"[FastDD] done={status['done_files']}/{status['discovered_files']} "
                        f"speed={human_bytes(status['rate_bps'])}/s failed={status['failed_files']}", '36', color), flush=True)
            last_log = time.monotonic()
        done.wait(1)


def command_run(args):
    if not hasattr(os, 'pread') or not hasattr(os, 'pwrite'):
        raise RuntimeError('fastdd requires Linux or macOS (POSIX pread/pwrite)')
    expanded = []
    for pattern in args.paths:
        matches = glob.glob(os.path.expanduser(pattern), recursive=True)
        if not matches:
            print(paint(f'Warning: no match: {safe_text(pattern)}', '33', use_color(args.color, sys.stderr)), file=sys.stderr)
        expanded.extend(matches)
    roots = remove_nested_roots([p for p in expanded if not os.path.islink(p)])
    if not roots:
        raise ValueError('no input paths matched (symlinks are excluded)')
    state = os.path.abspath(os.path.expanduser(args.state))
    os.makedirs(os.path.dirname(state), exist_ok=True)
    with state_lock(state + '.lock'):
        return run_locked(args, roots, state)


def run_locked(args, roots, state):
    db = init_db(state + '.sqlite')
    policy = (RetentionPolicy(args.retention_days, args.skip_if_remaining_days)
              if args.skip_if_remaining_days is not None else None)
    # Previously completed files may have aged back into the processing window.
    # With a policy, current mtime determines eligibility instead of old records.
    cache = {} if args.no_resume or policy else load_completed(db, args.mode)
    stats = Stats(args.mode, args.workers, policy)
    if policy and args.mode == 'read':
        print('Note: read mode does not refresh mtime; estimated cleanup deadlines remain unchanged.',
              file=sys.stderr, flush=True)
    work = queue.Queue(maxsize=args.queue_size)
    stop = threading.Event()
    writer_done = threading.Event()
    db_lock = threading.Lock()
    color = use_color(args.color)
    status_path = state + '.status.json'
    excluded = [state + suffix for suffix in (
        '.sqlite', '.sqlite-wal', '.sqlite-shm', '.sqlite-journal',
        '.status.json', '.status.json.tmp', '.lock')]
    previous_handlers = {}
    threads = []
    writer = threading.Thread(target=status_writer, args=(stats, status_path, writer_done, color))

    def handle_signal(signum, frame):
        stats.interrupted = True
        stop.set()

    def scan_error(error):
        with stats.lock:
            stats.failed_files += 1
            stats.errors.append({'path': getattr(error, 'filename', '') or '', 'error': str(error), 'time': time.time()})

    try:
        # Fail before launching workers if the state cannot be written.
        atomic_json(status_path, stats.snapshot())
        for sig in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[sig] = signal.signal(sig, handle_signal)
        writer.start()
        for worker_id in range(1, args.workers + 1):
            thread = threading.Thread(target=worker_loop, args=(
                worker_id, work, stats, stop, args.mode, args.chunk_mb * 1024 * 1024,
                args.fsync, db, db_lock, cache))
            thread.start()
            threads.append(thread)
        for path in iter_files(roots, excluded, scan_error):
            if stop.is_set():
                break
            try:
                st = os.stat(path, follow_symlinks=False)
            except OSError as error:
                scan_error(error)
                continue
            with stats.lock:
                stats.discovered_files += 1
                stats.discovered_bytes += st.st_size
            if policy:
                timing = policy.describe(st.st_mtime, time.time())
                if timing['skip']:
                    with stats.lock:
                        stats.skipped_files += 1
                        stats.skipped_bytes += st.st_size
                        stats.time_skipped_files += 1
                        stats.time_skipped_bytes += st.st_size
                        stats.recent_files.append(dict(timing, path=path, action='SKIPPED'))
                    continue
            with db_lock:
                previous = cache.get(path)
            if not policy and previous == (st.st_size, st.st_mtime_ns):
                with stats.lock:
                    stats.skipped_files += 1
                    stats.skipped_bytes += st.st_size
                continue
            while not stop.is_set():
                try:
                    work.put((path, st.st_size), timeout=0.1)
                    with stats.lock:
                        stats.queued = work.qsize()
                    break
                except queue.Full:
                    pass
        with stats.lock:
            stats.scan_done = not stop.is_set()
        # Unlike Queue.join(), this wait responds to interruption with a full queue.
        while not stop.is_set():
            with work.all_tasks_done:
                if work.unfinished_tasks == 0:
                    break
                work.all_tasks_done.wait(0.1)
    finally:
        stop.set()
        # Finish the current syscall/chunk before closing the shared database.
        for thread in threads:
            thread.join()
        writer_done.set()
        if writer.ident is not None:
            writer.join()
        with stats.lock:
            stats.finished = True
            stats.queued = work.qsize()
        final = stats.snapshot()
        db.close()
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)
        atomic_json(status_path, final)
        display(final, color, clear=True)
    return 130 if final['interrupted'] else (1 if final['failed_files'] else 0)


def command_watch(args):
    path = os.path.abspath(os.path.expanduser(args.state)) + '.status.json'
    try:
        while True:
            try:
                with open(path) as handle:
                    status = json.load(handle)
            except (FileNotFoundError, json.JSONDecodeError):
                if args.once:
                    raise ValueError(f'no readable status file: {path}') from None
                print(f'Waiting for {safe_text(path)} ...', flush=True)
                time.sleep(args.interval)
                continue
            display(status, use_color(args.color), clear=True)
            if args.once or status.get('finished'):
                return 0
            pid = status.get('pid')
            if pid:
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    print('FastDD process has exited; showing its last saved status.')
                    return 0
                except PermissionError:
                    pass
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print('\nWatch exited. Background FastDD is unchanged.')
        return 0


def build_parser():
    parser = argparse.ArgumentParser(prog='fastdd', description='Parallel file reader/rewriter with resume and colorful live status.')
    parser.add_argument('--version', action='version', version=f'%(prog)s {__version__}')
    parser.add_argument('--color', choices=('auto', 'always', 'never'), default='auto', help='ANSI colors (default: auto; respects NO_COLOR)')
    sub = parser.add_subparsers(dest='command', required=True)
    run = sub.add_parser('run', help='read or rewrite files')
    run.add_argument('paths', nargs='+', help='files, directories or quoted glob patterns')
    run.add_argument('--mode', choices=('read', 'rewrite'), default='read')
    run.add_argument('--workers', type=int, default=min(16, max(4, (os.cpu_count() or 8) // 2)))
    run.add_argument('--chunk-mb', type=int, default=32)
    run.add_argument('--queue-size', type=int, default=256)
    run.add_argument('--state', required=True, help='prefix for SQLite and status files')
    run.add_argument('--fsync', action='store_true', help='fsync each rewritten file before marking it complete')
    run.add_argument('--no-resume', action='store_true', help='process all files even if previously completed')
    run.add_argument('--retention-days', type=float, default=90,
                     help='estimated cleanup age since mtime (default: 90 days; used with --skip-if-remaining-days)')
    run.add_argument('--skip-if-remaining-days', type=float, metavar='DAYS',
                     help='enable mtime policy: skip files with more than DAYS left; process all others, including previously completed files')
    run.set_defaults(func=command_run)
    watch = sub.add_parser('watch', help='view live or completed status')
    watch.add_argument('--state', required=True)
    watch.add_argument('--interval', type=float, default=1.0)
    watch.add_argument('--once', action='store_true', help='print one snapshot and exit')
    watch.set_defaults(func=command_watch)
    for child in (run, watch):
        child.add_argument('--color', choices=('auto', 'always', 'never'), default=argparse.SUPPRESS, help='override color policy')
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    for name in ('workers', 'chunk_mb', 'queue_size'):
        if getattr(args, name, 1) < 1:
            parser.error(f'--{name.replace("_", "-")} must be >= 1')
    interval = getattr(args, 'interval', 1.0)
    if not math.isfinite(interval) or interval <= 0:
        parser.error('--interval must be a finite number > 0')
    retention = getattr(args, 'retention_days', 90)
    if not math.isfinite(retention) or retention <= 0:
        parser.error('--retention-days must be a finite number > 0')
    threshold = getattr(args, 'skip_if_remaining_days', None)
    if threshold is not None and (not math.isfinite(threshold) or not 0 <= threshold <= retention):
        parser.error('--skip-if-remaining-days must be between 0 and --retention-days')
    try:
        return args.func(args)
    except (OSError, ValueError, RuntimeError) as error:
        print(paint(f'fastdd: {safe_text(error)}', '31', use_color(args.color, sys.stderr)), file=sys.stderr)
        return 2
