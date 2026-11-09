"""ANSI colors without third-party dependencies."""

import os
import sys

from .core import human_bytes, human_time
from .retention import calendar_time


def use_color(policy='auto', stream=None):
    stream = sys.stdout if stream is None else stream
    if policy != 'auto':
        return policy == 'always'
    return stream.isatty() and 'NO_COLOR' not in os.environ and os.environ.get('TERM') != 'dumb'


def paint(text, code, enabled):
    return f'\033[{code}m{text}\033[0m' if enabled else str(text)


def safe_text(value):
    # Filenames and errors must not inject terminal control sequences.
    return ''.join(c if c.isprintable() else repr(c)[1:-1] for c in str(value))


def file_time_line(info):
    remaining = info.get('remaining_days')
    if remaining is None:
        return ''
    left = f'{remaining:.2f} days left' if remaining >= 0 else f'{-remaining:.2f} days overdue'
    return (f"    mtime {calendar_time(info.get('mtime'))} | "
            f"est. cleanup {calendar_time(info.get('estimated_expiry'))} | {left}")


def render_status(status, color=False):
    p = lambda value, code: paint(value, code, color)
    percent = status.get('percent')
    count = int(min(100, max(0, percent or 0)) * 0.4)
    bar = p('━' * count, '32') + p('─' * (40 - count), '90')
    phase = 'SCANNING' if not status.get('scan_done') else 'RUNNING'
    if status.get('finished'):
        phase = 'INTERRUPTED' if status.get('interrupted') else ('FAILED' if status.get('failed_files') else 'COMPLETE')
    phase_color = '31' if phase == 'FAILED' else ('32' if phase == 'COMPLETE' else '33')
    lines = [
        '', p('  FastDD', '1;36') + '  ' + p(phase, phase_color),
        f"  PID {status.get('pid')} · {status.get('mode')} · {status.get('workers')} workers",
        f"  Started  {calendar_time(status.get('started'))}",
        f"  Updated  {calendar_time(status.get('updated'))}",
        '', f"  {bar}  {p(f'{percent:.2f}%' if percent is not None else 'scanning', '1;36')}",
        '',
        f"  Found    {status.get('discovered_files', 0):,} files / {human_bytes(status.get('discovered_bytes', 0))}",
        f"  Done     {p(status.get('done_files', 0), '32')}    Skipped {p(status.get('skipped_files', 0), '33')}    Failed {p(status.get('failed_files', 0), '31' if status.get('failed_files') else '90')}",
        f"  Completed {human_bytes(status.get('done_bytes', 0))}",
        f"  Speed    {p(human_bytes(status.get('rate_bps', 0)) + '/s', '1;36')}    ETA {human_time(status.get('eta_seconds'))}",
        f"  Elapsed  {human_time(status.get('elapsed_seconds', 0))}    Queue {status.get('queued', 0):,}",
    ]
    policy = status.get('retention_policy')
    if policy:
        lines.extend([
            '',
            f"  Cleanup estimate: mtime + {policy['retention_days']:g} days; skip when > {policy['skip_if_remaining_days']:g} days left",
            f"  Time skipped  {p(status.get('time_skipped_files', 0), '33')} files / {human_bytes(status.get('time_skipped_bytes', 0))}",
        ])
        if status.get('mode') == 'read':
            lines.append(p('  Read mode does not refresh mtime.', '33'))
    active = status.get('active', {})
    if active:
        lines.extend(['', p('  Active files', '1;34')])
        for worker, info in list(active.items())[:10]:
            path = safe_text(info.get('path', ''))
            lines.append(f"  W{worker:>2}  {human_bytes(info.get('processed', 0))} / {human_bytes(info.get('size', 0))}  {path[-90:]}")
            if info.get('remaining_days') is not None:
                lines.append(file_time_line(info))
    recent = status.get('recent_files', [])
    if recent:
        lines.extend(['', p('  Recent file decisions (times at decision/completion)', '1;34')])
        for info in recent[-3:]:
            action = info.get('action', '')
            lines.append(f"  {p(action, '32' if action == 'DONE' else '33')}  {safe_text(info.get('path', ''))[-90:]}")
            lines.append(file_time_line(info))
    errors = status.get('errors', [])
    if errors:
        lines.extend(['', p('  Recent errors', '1;31')])
        for error in errors[-3:]:
            lines.append(p(f"  {safe_text(error.get('path', ''))}: {safe_text(error.get('error', ''))}", '31'))
    return '\n'.join(lines) + '\n'
