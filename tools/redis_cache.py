# tools/redis_cache.py

import os
import redis
from tools.config import get_config

# ─── Load REDIS configuration from centralized config system ────────────────
config = get_config()
redis_config = config.get_service_config('redis')

REDIS_HOST = redis_config.get("host", "localhost")
REDIS_PORT = redis_config.get("port", 6379)
REDIS_DB = redis_config.get("db", 0)
REDIS_TTL = redis_config.get("ttl", 600)

# initialize client
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

def set_latest_quote(symbol: str, data: str):
    """
    Overwrite the latest quote JSON under key SPX:QUOTE:<symbol>,
    with an expiration so stale symbols auto-vanish.

    TTL is controlled by REDIS_TTL (seconds).
    """
    key = f"SPX:QUOTE:{symbol}"
    r.set(key, data, ex=REDIS_TTL)

def get_latest_quote(symbol: str) -> str | None:
    """
    Retrieve the most recent quote for a symbol, or None if it has expired.
    """
    key = f"SPX:QUOTE:{symbol}"
    val = r.get(key)
    return val.decode() if val else None
