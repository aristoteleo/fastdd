"""Estimate cleanup deadlines from the user-specified mtime policy."""

from dataclasses import dataclass
from datetime import datetime


DAY_SECONDS = 86400


@dataclass(frozen=True)
class RetentionPolicy:
    retention_days: float = 90
    skip_if_remaining_days: float = 30

    def describe(self, mtime, now):
        expires = mtime + self.retention_days * DAY_SECONDS
        remaining = (expires - now) / DAY_SECONDS
        return {
            'mtime': mtime,
            'estimated_expiry': expires,
            'remaining_days': remaining,
            'skip': remaining > self.skip_if_remaining_days,
        }


def calendar_time(timestamp):
    if timestamp is None:
        return '--'
    try:
        return datetime.fromtimestamp(timestamp).astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')
    except (ValueError, OverflowError, OSError):
        return str(timestamp)
