"""One shared concurrency budget for monitoring database reads."""
from contextlib import contextmanager
from threading import BoundedSemaphore

from fastapi import HTTPException

_reads = BoundedSemaphore(8)


@contextmanager
def monitoring_read():
    if not _reads.acquire(timeout=5):
        raise HTTPException(503, "Monitoring data is busy. Try again shortly.")
    try:
        yield
    finally:
        _reads.release()
