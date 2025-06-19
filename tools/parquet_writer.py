# tools/parquet_writer.py

import os
import atexit
import pandas as pd
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

PARQUET_DIR = os.getenv("PARQUET_DIR", "./parquet")
os.makedirs(PARQUET_DIR, exist_ok=True)

class ParquetWriter:
    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size
        self.buffer = []
        self._last_date = None
        atexit.register(self.close)

    def write(self, record: dict):
        self.buffer.append(record)
        # If date rolled over since last flush, force a flush
        today = pd.Timestamp.now().strftime("%Y-%m-%d")
        if self._last_date and self._last_date != today:
            self.flush()
        if len(self.buffer) >= self.batch_size:
            self.flush()

    def flush(self):
        date_str = pd.Timestamp.now().strftime("%Y-%m-%d")
        path = os.path.join(PARQUET_DIR, f"quotes_{date_str}.parquet")

        # 1) On first flush of the day, create an empty file if none exists
        if self._last_date != date_str and not os.path.exists(path):
            pd.DataFrame().to_parquet(path, engine="pyarrow", compression="snappy")

        # 2) Append buffered records if any
        if self.buffer:
            df = pd.DataFrame(self.buffer)
            if os.path.exists(path):
                existing = pd.read_parquet(path)
                df = pd.concat([existing, df], ignore_index=True)
            df.to_parquet(path, engine="pyarrow", compression="snappy")
            self.buffer.clear()

        self._last_date = date_str

    def close(self):
        """Flush any remaining records before shutdown."""
        self.flush()
