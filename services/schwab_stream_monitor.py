#!/usr/bin/env python3
"""
tools/schwab_stream_monitor.py

Simplified data quality monitor for SchwabStream system.
Prints a single-line report every POLL_INTERVAL seconds with four metrics.
Runs continuously when started by controller (no market hours logic).
Emails are sent only if there are two freshness violations in a row.
"""

import os
import time
import logging
import json
import sys
from datetime import datetime

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

import pandas as pd
import redis
from tools.emailer import send_email

# ─── Load configuration from centralized config system ──────────────────────
from tools.config import get_config

config = get_config()
redis_config = config.get_service_config('redis')
stream_config = config.get_service_config('stream')

REDIS_HOST = redis_config.get("host", "localhost")
REDIS_PORT = redis_config.get("port", 6379)
REDIS_DB = redis_config.get("db", 0)
REDIS_FRESHNESS = stream_config.get("redis_freshness", 60)      # seconds
PARQUET_DIR = stream_config.get("parquet_dir", "./parquet")
PARQUET_FRESHNESS = stream_config.get("parquet_freshness", 120) # seconds
POLL_INTERVAL = stream_config.get("poll_interval", 30)          # seconds for monitoring checks

# Redis key patterns (constants)
SPX_REDIS_KEY = "SPX:QUOTE:$SPX"
SPXW_PATTERN = "SPX:QUOTE:SPXW*"

# ─── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def check_redis():
    """
    Returns:
      spx_fresh (bool): True if SPX key exists and is updated within REDIS_FRESHNESS seconds
      spxw_count (int): number of SPXW keys updated within REDIS_FRESHNESS seconds
    """
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    now_ms = int(time.time() * 1000)

    # SPX
    spx_fresh = False
    try:
        raw_spx = r.get(SPX_REDIS_KEY)
        if raw_spx:
            data_spx = raw_spx.decode("utf-8", errors="ignore")
            try:
                obj = json.loads(data_spx)
            except Exception:
                obj = eval(data_spx)
            ts_spx = obj.get("35")
            if ts_spx is not None:
                age_spx = (now_ms - int(ts_spx)) / 1000
                spx_fresh = (age_spx <= REDIS_FRESHNESS)
    except Exception:
        spx_fresh = False

    # SPXW options
    spxw_count = 0
    try:
        keys = r.keys(SPXW_PATTERN)
        for raw_key in keys:
            raw_val = r.get(raw_key)
            if not raw_val:
                continue
            decoded = raw_val.decode("utf-8", errors="ignore")
            try:
                obj = json.loads(decoded)
            except Exception:
                obj = eval(decoded)
            ts = obj.get("38")
            if ts is not None:
                age = (now_ms - int(ts)) / 1000
                if age <= REDIS_FRESHNESS:
                    spxw_count += 1
    except Exception:
        spxw_count = 0

    return spx_fresh, spxw_count

def check_parquet():
    """
    Returns:
      spx_fresh_parquet (bool): True if there is an SPX record in today's parquet updated within PARQUET_FRESHNESS seconds
      spxw_count_parquet (int): number of SPXW records in today's parquet updated within PARQUET_FRESHNESS seconds
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"quotes_{today_str}.parquet"
    path = os.path.join(PARQUET_DIR, filename)
    now_ms = int(time.time() * 1000)

    if not os.path.exists(path):
        return False, 0

    try:
        df = pd.read_parquet(path)
    except Exception:
        return False, 0

    df["age_sec"] = (now_ms - df["received_at"]) / 1000

    spx_mask = (df["symbol"] == "$SPX") & (df["age_sec"] <= PARQUET_FRESHNESS)
    spx_fresh = spx_mask.any()

    spxw_mask = df["symbol"].str.startswith("SPXW") & (df["age_sec"] <= PARQUET_FRESHNESS)
    spxw_count = int(spxw_mask.sum())

    return spx_fresh, spxw_count

def main():
    """
    Simplified main loop - runs continuously when started by controller.
    No market hours logic - trusts controller to start/stop at appropriate times.
    """
    logging.info(
        "Starting simplified monitor: Redis freshness %ds, Parquet freshness %ds, poll every %ds",
        REDIS_FRESHNESS, PARQUET_FRESHNESS, POLL_INTERVAL
    )

    prev_violation = False
    running = True

    try:
        while running:
            # Check data freshness
            spx_fresh_redis, spxw_count_redis = check_redis()
            spx_fresh_pq, spxw_count_pq = check_parquet()

            report = (
                f"Redis → SPX fresh: {spx_fresh_redis}, "
                f"SPXW count fresh: {spxw_count_redis} | "
                f"Parquet → SPX fresh: {spx_fresh_pq}, "
                f"SPXW count fresh: {spxw_count_pq}"
            )
            logging.info(report)

            violation = (
                (not spx_fresh_redis) or
                (spxw_count_redis == 0) or
                (not spx_fresh_pq) or
                (spxw_count_pq == 0)
            )

            if violation and prev_violation:
                subject = "ALERT: Data freshness issue detected"
                body = (
                    f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Freshness check failed two times in a row:\n"
                    f"{report}"
                )
                send_email(subject, body)
                prev_violation = False
            elif violation:
                prev_violation = True
            else:
                prev_violation = False

            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        logging.info("Monitor stopped by user")
    except Exception as e:
        logging.error("Monitor error: %s", e)
    finally:
        logging.info("Monitor shutting down")

if __name__ == "__main__":
    main()
