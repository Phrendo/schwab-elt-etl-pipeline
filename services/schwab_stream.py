# tools/schwab_stream.py

import json, time, threading, websocket, logging, os, sys
from datetime import date, datetime
# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)


from tools.db             import DB
from tools.redis_cache    import set_latest_quote
from tools.parquet_writer import ParquetWriter
from tools.schwab         import SchwabAPI, generate_spxw_symbols

# ─── Load configuration from centralized config system ──────────────────────
from tools.config import get_config

config = get_config()
app_config = config.get_service_config('application')
api_config = app_config.get('api', {})
stream_config = config.get_service_config('stream')

STRIKE_RANGE = stream_config.get("strike_range", 100.0)         # points
STRIKE_STEP = stream_config.get("strike_step", 5.0)             # points
NO_DATA_THRESHOLD = stream_config.get("no_data_threshold", 30)  # seconds
ADJUST_THRESHOLD = stream_config.get("adjust_threshold", 30.0)  # points
BACKOFF_SECONDS = stream_config.get("backoff_seconds", 10)      # seconds between reconnection attempts
SPX_UNDERLYING = "$SPX"  # This is a constant, not configurable
API_DATA_NAME = api_config.get("data_name", "MAIN_DATA")
API_TRADE_NAME = api_config.get("trade_name", "MAIN_TRADE")

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


class SchwabStream:
    """
    Simplified WebSocket consumer for SPX options and underlying ticks.
    Runs until 13:00 PM and handles reconnections during that time.
    """

    def __init__(self):
        # Instantiate unified SchwabAPI (holds both DATA and TRADE tokens)
        logging.info("Initializing SchwabStream instance")
        # Use unified SchwabAPI with separate data and trade API names
        self.client = SchwabAPI(API_DATA_NAME, data_name=API_DATA_NAME, trade_name=API_TRADE_NAME)

        # Initialize database connection for token retrieval
        self.db = DB()

        # Fetch streamerInfo via TRADE token
        try:
            logging.info("Fetching user preferences (streamerInfo) via TRADE token")
            prefs = self.client.get_user_preferences()
            info = prefs["streamerInfo"][0]
            self.stream_url  = info["streamerSocketUrl"]
            self.client_cust = info["schwabClientCustomerId"]
            self.client_corr = info["schwabClientCorrelId"]
            self.client_chan = info["schwabClientChannel"]
            self.client_func = info["schwabClientFunctionId"]
            logging.info("Successfully fetched streamerInfo: stream_url=%s", self.stream_url)
        except Exception as e:
            logging.error("Failed to fetch user preferences: %s", e)
            raise

        self.parquet_writer          = ParquetWriter()
        self.last_msg_ts             = time.time()
        self.initial_spx_price       = None
        self.current_spx_price       = None
        self.running                 = True
        self.spx_fail_count          = 0

        # Connection state management
        self.connection_lock         = threading.Lock()
        self.is_connecting           = False
        self.is_connected            = False
        self.ws_app                  = None

        # Load today’s (or next) market hours
        self.end_time = self._calculate_end_time()

    def _calculate_end_time(self):
        """Calculate today's end time (13:00 PM) as timestamp."""
        from datetime import time as dt_time
        today = datetime.now().date()
        end_time = dt_time(13, 0)  # 13:00 PM
        end_datetime = datetime.combine(today, end_time)
        return end_datetime.timestamp()



    def _watchdog(self):
        """
        Monitors incoming data. If no new quote arrives for > NO_DATA_THRESHOLD,
        or SPX moves > ADJUST_THRESHOLD, forcibly close ws to trigger reconnect.
        Runs every 30 seconds. Always active during runtime.
        """
        while self.running:
            time.sleep(30)
            if not self.running:
                break

            # Use connection lock to prevent race conditions
            with self.connection_lock:
                # Skip watchdog actions if we're in the middle of connecting
                if self.is_connecting:
                    logging.debug("WATCHDOG: Skipping check - connection in progress")
                    continue

                # Skip if no active connection
                if not self.is_connected or self.ws_app is None:
                    logging.debug("WATCHDOG: Skipping check - no active connection")
                    continue

                age = time.time() - self.last_msg_ts
                should_reconnect = False

                if age > NO_DATA_THRESHOLD:
                    logging.warning("WATCHDOG: No data for %ds—forcing reconnect", int(age))
                    should_reconnect = True

                if (self.initial_spx_price is not None and self.current_spx_price is not None
                        and abs(self.current_spx_price - self.initial_spx_price) >= ADJUST_THRESHOLD):
                    logging.warning(
                        "WATCHDOG: SPX moved from %.2f to %.2f ≥ %.2f—forcing reconnect",
                        self.initial_spx_price, self.current_spx_price, ADJUST_THRESHOLD
                    )
                    should_reconnect = True

                if should_reconnect:
                    try:
                        logging.info("WATCHDOG: Initiating controlled disconnect")
                        self.is_connected = False  # Mark as disconnected before closing
                        self.ws_app.close()
                    except Exception as ex:
                        logging.error("WATCHDOG: Failed to close ws_app: %s", ex)

    def on_open(self, ws: websocket.WebSocketApp):
        """
        Called once WebSocket is open:
        1) Reset SPX failure counter.
        2) Refresh DATA token.
        3) Fetch SPX quote (retry once on 401).
        4) If success → generate symbols, send LOGIN, subscribe to options+SPX.
        5) On fetch failures, increment counter, send email after 10 fails, close socket.
        """
        logging.info("on_open: WebSocket opened, refreshing DATA token and fetching SPX quote")

        # Set connection state under lock
        with self.connection_lock:
            self.is_connecting = True
            self.is_connected = False

        self.spx_fail_count = 0

        # Refresh DATA token
        try:
            new_token = self.db.get_token(API_DATA_NAME)
            self.client.token_data = new_token
            logging.info("on_open: Successfully refreshed DATA token")
        except Exception as e:
            logging.error("on_open: Failed to refresh DATA token: %s", e)
            with self.connection_lock:
                self.is_connecting = False
            ws.close()
            return

        # Fetch SPX quote, retry on 401
        try:
            price = self._fetch_spx_quote_with_retry()
            logging.info("on_open: Fetched SPX price=%.2f", price)
        except Exception:
            self.spx_fail_count += 1
            logging.error("on_open: SPX fetch failed (count=%d)", self.spx_fail_count)
            with self.connection_lock:
                self.is_connecting = False
            ws.close()
            return

        # Reset failure count
        self.spx_fail_count = 0

        self.initial_spx_price = price
        self.current_spx_price = price

        # Generate SPX weekly option symbols
        expiry = date.today()
        self.option_symbols = generate_spxw_symbols(
            spx_price   = price,
            range_width = STRIKE_RANGE,
            strike_step = STRIKE_STEP,
            expiry_date = expiry
        )
        logging.info(
            "on_open: Generated %d option symbols for expiry %s",
            len(self.option_symbols), expiry
        )

        # ADMIN LOGIN using TRADE token
        login_payload = {
            "service": "ADMIN",
            "command": "LOGIN",
            "requestid": 0,
            "SchwabClientCustomerId": self.client_cust,
            "SchwabClientCorrelId":    self.client_corr,
            "parameters": {
                "Authorization":         self.client.token_trade,
                "SchwabClientChannel":   self.client_chan,
                "SchwabClientFunctionId":self.client_func
            }
        }
        logging.info("on_open: Sending ADMIN LOGIN payload")
        ws.send(json.dumps({"requests": [login_payload]}))
        time.sleep(0.5)  # allow Schwab to process LOGIN

        # Subscribe to OPTIONS
        opts_payload = {
            "service": "LEVELONE_OPTIONS",
            "command": "SUBS",
            "requestid": 1,
            "SchwabClientCustomerId": self.client_cust,
            "SchwabClientCorrelId":    self.client_corr,
            "parameters": {
                "keys":   ",".join(self.option_symbols),
                "fields": "0,37,38"
            }
        }
        logging.info("on_open: Subscribing to %d option symbols", len(self.option_symbols))
        ws.send(json.dumps({"requests": [opts_payload]}))

        # Subscribe to SPX underlying ticks
        idx_payload = {
            "service": "LEVELONE_EQUITIES",
            "command": "SUBS",
            "requestid": 2,
            "SchwabClientCustomerId": self.client_cust,
            "SchwabClientCorrelId":    self.client_corr,
            "parameters": {
                "keys":   SPX_UNDERLYING,
                "fields": "0,3,35"
            }
        }
        logging.info("on_open: Subscribing to %s ticks", SPX_UNDERLYING)
        ws.send(json.dumps({"requests": [idx_payload]}))

        # Mark connection as fully established
        with self.connection_lock:
            self.is_connecting = False
            self.is_connected = True
        logging.info("on_open: Connection setup complete")

    def _fetch_spx_quote_with_retry(self) -> float:
        """
        Attempt to fetch SPX underlying quote. On 401:
         - Refresh DATA token once and retry.
         - Raise if second attempt fails.
        """
        try:
            return self.client.get_underlying_quote(SPX_UNDERLYING)
        except Exception as e:
            if hasattr(e, "response") and e.response.status_code == 401:
                logging.warning("fetch_spx: 401 fetching SPX—refreshing DATA token & retrying")
                self.client.token_data = self.db.get_token(API_DATA_NAME)
                try:
                    return self.client.get_underlying_quote(SPX_UNDERLYING)
                except Exception as e2:
                    logging.error("fetch_spx: Retry SPX quote failed: %s", e2)
                    raise
            else:
                raise

    def on_message(self, ws: websocket.WebSocketApp, message: str):
        """
        Handle incoming WebSocket messages. Parse JSON, update last_msg_ts,
        write to Redis + Parquet, or close on fatal errors.
        """
        msg = json.loads(message)
        for blk in msg.get("data", []):
            service = blk.get("service")

            if service == "ADMIN":
                logging.info("on_message: ADMIN response: %s", blk.get("content", {}))
                continue

            if service == "Invalid Service":
                logging.error("on_message: Invalid Service detected—closing ws")
                ws.close()
                return

            if service == "HEARTBEAT":
                return  # ignore heartbeats

            if service not in ("LEVELONE_OPTIONS", "LEVELONE_EQUITIES"):
                logging.info("on_message: Other service=%s; content=%s", service, json.dumps(blk))
                continue

            if service == "LEVELONE_OPTIONS":
                for content in blk.get("content", []):
                    key = content.get("key")
                    if content.get("37") is None:
                        continue
                    self.last_msg_ts = time.time()
                    set_latest_quote(key, json.dumps(content))
                    record = {
                        "received_at": int(self.last_msg_ts * 1000),
                        "symbol":      key,
                        **{k: content[k] for k in content if k != "key"}
                    }
                    self.parquet_writer.write(record)

            elif service == "LEVELONE_EQUITIES":
                for content in blk.get("content", []):
                    key = content.get("key")
                    if key != SPX_UNDERLYING:
                        continue

                    self.last_msg_ts = time.time()
                    raw_price = content.get("3")
                    if raw_price in (None, ""):
                        logging.debug("on_message: Empty SPX tick; skipping")
                        return

                    try:
                        price = float(raw_price)
                    except (TypeError, ValueError):
                        logging.warning("on_message: Could not parse SPX price: %r", raw_price)
                        return

                    self.current_spx_price = price
                    set_latest_quote(key, json.dumps(content))
                    record = {
                        "received_at": int(self.last_msg_ts * 1000),
                        "symbol":      key,
                        **{k: content[k] for k in content if k != "key"}
                    }
                    self.parquet_writer.write(record)

    def on_error(self, ws, error):
        """
        Called on any low-level WebSocket error. Logs error details.
        """
        logging.error("on_error: WebSocket error: %s", error)

    def on_close(self, ws, code, reason):
        """
        Called when WebSocket closes. Logs code & reason.
        """
        # Reset connection state on close
        with self.connection_lock:
            self.is_connecting = False
            self.is_connected = False
        logging.info("on_close: WebSocket closed: code=%s reason=%s", code, reason)

    def run(self):
        """
        Simplified main loop:
         1) Start watchdog thread
         2) Reset counters and start streaming
         3) Keep reconnecting until end_time (13:00 PM)
         4) Self-terminate at end_time
        """
        logging.info("Starting simplified run() loop - will run until 13:00 PM")
        threading.Thread(target=self._watchdog, daemon=True).start()

        try:
            # Reset counters at start
            self.last_msg_ts = time.time()
            self.initial_spx_price = None
            self.current_spx_price = None
            self.spx_fail_count = 0

            # Keep reconnecting until end_time
            while time.time() < self.end_time and self.running:
                now_ts = time.time()
                logging.info(
                    "Entering connect loop: now=%s   end_time=%s",
                    datetime.fromtimestamp(now_ts).strftime("%Y-%m-%d %H:%M:%S"),
                    datetime.fromtimestamp(self.end_time).strftime("%Y-%m-%d %H:%M:%S")
                )

                logging.info("WebSocket connecting…")
                self.ws_app = websocket.WebSocketApp(
                    self.stream_url,
                    on_open    = self.on_open,
                    on_message = self.on_message,
                    on_error   = self.on_error,
                    on_close   = self.on_close
                )
                try:
                    self.ws_app.run_forever(ping_interval=30, ping_timeout=10)
                except Exception as e:
                    logging.error("run_forever() crashed: %s", e)

                post_ts = time.time()
                logging.info(
                    "run_forever() returned at %s",
                    datetime.fromtimestamp(post_ts).strftime("%Y-%m-%d %H:%M:%S")
                )

                # If end time reached, break out
                if post_ts >= self.end_time:
                    logging.info("Reached end_time (13:00 PM) - terminating")
                    break

                # Otherwise, we disconnected prematurely; wait before retry
                if self.running:
                    logging.info("Disconnected prematurely—sleeping %d seconds before retry", BACKOFF_SECONDS)
                    time.sleep(BACKOFF_SECONDS)

            logging.info("Stream session completed - shutting down")

        finally:
            self.running = False
            logging.info("Shutting down—flushing ParquetWriter")
            self.parquet_writer.close()


if __name__ == "__main__":
    SchwabStream().run()
