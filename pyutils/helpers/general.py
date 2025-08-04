import datetime
import time

import rfc3339


def current_utc():
    return datetime.datetime.fromtimestamp(time.time(), datetime.timezone.utc)


def format_rfc3339(timestamp, microseconds=False):
    formatter = rfc3339.rfc3339
    if microseconds:
        formatter = rfc3339.format_microsecond
    return formatter(timestamp, utc=True)


def current_utc_rfc3339(microseconds=False):
    current_time = current_utc()
    return format_rfc3339(current_time, microseconds)
