import logging
import os
import time
import functools
from logging.handlers import RotatingFileHandler

LOG_DIR = os.path.join(os.path.dirname(__file__), "log")
os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger("ordenate")
logger.setLevel(logging.INFO)
if not logger.handlers:
    log_path = os.path.join(LOG_DIR, "app.log")
    handler = RotatingFileHandler(log_path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def _summarize_val(v):
    try:
        if isinstance(v, (str, int, float, bool)):
            return repr(v)
        # avoid expensive repr for large objects (DataFrame, etc.)
        return f"<{type(v).__name__}>"
    except Exception:
        return f"<{type(v).__name__}>"


def _summarize_args(args, kwargs):
    parts = []
    for a in args:
        parts.append(_summarize_val(a))
    for k, v in kwargs.items():
        parts.append(f"{k}={_summarize_val(v)}")
    return ", ".join(parts)


def log_time(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            logger.info(f"START {func.__name__}({ _summarize_args(args, kwargs) })")
        except Exception:
            logger.info(f"START {func.__name__}")
        start = time.perf_counter()
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            elapsed = time.perf_counter() - start
            logger.info(f"END {func.__name__} elapsed={elapsed:.6f}s")

    return wrapper


def get_logger():
    return logger
