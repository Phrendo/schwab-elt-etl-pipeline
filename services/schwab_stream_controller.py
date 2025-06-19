#!/usr/bin/env python3
"""
tools/schwab_stream_controller.py

Controller for SchwabStream system - handles timing, scheduling, and process lifecycle.
Manages both schwab_stream.service and schwab_stream_monitor.service.

Features:
- Loads market hours from database for scheduling
- Starts stream at 06:30 AM on trading days
- Starts monitor during market hours
- Force terminates processes at 13:00:10 PM
- Monitors process health and restarts if crashed
- Handles holidays and market closures
"""

import os
import time
import logging
import subprocess
import signal
import sys
from datetime import datetime, date, time as dt_time, timedelta

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from dotenv import load_dotenv, find_dotenv
from tools.db import DB

# ─── Load .env ────────────────────────────────────────────────────────────────
load_dotenv(find_dotenv())

# ─── Configuration from environment variables ─────────────────────────────────
# Timing configuration
MARKET_START_TIME = os.getenv("MARKET_START_TIME", "06:30")    # When controller starts stream
FORCE_KILL_TIME = os.getenv("FORCE_KILL_TIME", "13:00:10")     # When controller force-kills
NEXT_DAY_CHECK_TIME = os.getenv("NEXT_DAY_CHECK_TIME", "00:01")  # Next day check time

# Service management
STREAM_SERVICE_NAME = os.getenv("STREAM_SERVICE_NAME", "schwab-stream.service")
MONITOR_SERVICE_NAME = os.getenv("MONITOR_SERVICE_NAME", "schwab-stream-monitor.service")

# Market schedule configuration
MARKET_SCHEDULE_DAYS_AHEAD = int(os.getenv("MARKET_SCHEDULE_DAYS_AHEAD", "30"))
SCHEDULE_RELOAD_RETRY_HOURS = int(os.getenv("SCHEDULE_RELOAD_RETRY_HOURS", "1"))

# Service monitoring configuration
SERVICE_CHECK_INTERVAL = int(os.getenv("SERVICE_CHECK_INTERVAL", "60"))
ERROR_SLEEP_INTERVAL = int(os.getenv("ERROR_SLEEP_INTERVAL", "60"))

# Service management timeouts
SYSTEMCTL_TIMEOUT = int(os.getenv("SYSTEMCTL_TIMEOUT", "30"))
SERVICE_STATUS_TIMEOUT = int(os.getenv("SERVICE_STATUS_TIMEOUT", "10"))

# ─── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

class SchwabStreamController:
    """
    Controller for managing SchwabStream system lifecycle.
    """

    def __init__(self):
        self.running = True
        self.market_schedule = {}
        self.schedule_loaded_date = None  # Track when schedule was last loaded
        self.stream_running = False
        self.monitor_running = False

        # Initialize database connection
        self.db = DB()

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, _frame):
        """Handle shutdown signals gracefully."""
        logging.info(f"Received signal {signum}, shutting down...")
        self.running = False
        self._stop_all_services()
        sys.exit(0)
        
    def needs_schedule_reload(self):
        """Check if we need to reload the market schedule (only on date change)."""
        today = date.today()
        return self.schedule_loaded_date != today or not self.market_schedule

    def load_market_schedule(self):
        """Load market hours from database for current and future dates."""
        try:
            # Use DB class to load market schedule
            self.market_schedule = self.db.load_market_schedule(MARKET_SCHEDULE_DAYS_AHEAD)
            self.schedule_loaded_date = date.today()
            logging.info(f"Loaded market schedule for {len(self.market_schedule)} days")
            return True
        except Exception as e:
            logging.error(f"Failed to load market schedule: {e}")
            return False
        
    def get_next_trading_day(self):
        """Find the next trading day from the loaded schedule."""
        today = date.today()
        
        for market_date, schedule in self.market_schedule.items():
            if market_date >= today and schedule['is_open']:
                return market_date, schedule
                
        return None, None
        
    def is_service_running(self, service_name):
        """Check if a systemctl service is running."""
        try:
            result = subprocess.run(
                ['systemctl', 'is-active', service_name],
                capture_output=True, text=True, timeout=SERVICE_STATUS_TIMEOUT
            )
            return result.stdout.strip() == 'active'
        except Exception as e:
            logging.error(f"Error checking service {service_name}: {e}")
            return False
            
    def start_service(self, service_name):
        """Start a systemctl service."""
        try:
            result = subprocess.run(
                ['sudo', 'systemctl', 'start', service_name],
                capture_output=True, text=True, timeout=SYSTEMCTL_TIMEOUT
            )
            if result.returncode == 0:
                logging.info(f"Started service: {service_name}")
                return True
            else:
                logging.error(f"Failed to start {service_name}: {result.stderr}")
                return False
        except Exception as e:
            logging.error(f"Error starting service {service_name}: {e}")
            return False
            
    def stop_service(self, service_name):
        """Stop a systemctl service."""
        try:
            result = subprocess.run(
                ['sudo', 'systemctl', 'stop', service_name],
                capture_output=True, text=True, timeout=SYSTEMCTL_TIMEOUT
            )
            if result.returncode == 0:
                logging.info(f"Stopped service: {service_name}")
                return True
            else:
                logging.error(f"Failed to stop {service_name}: {result.stderr}")
                return False
        except Exception as e:
            logging.error(f"Error stopping service {service_name}: {e}")
            return False
            
    def _stop_all_services(self):
        """Stop both stream and monitor services."""
        if self.stream_running:
            self.stop_service(STREAM_SERVICE_NAME)
            self.stream_running = False
        if self.monitor_running:
            self.stop_service(MONITOR_SERVICE_NAME)
            self.monitor_running = False
            
    def calculate_sleep_time(self, target_time):
        """Calculate seconds to sleep until target time today."""
        now = datetime.now()
        today = now.date()
        target_datetime = datetime.combine(today, target_time)
        
        if target_datetime <= now:
            # Target time has passed today, return 0
            return 0
            
        return int((target_datetime - now).total_seconds())
        
    def run(self):
        """Main controller loop."""
        logging.info("SchwabStream Controller starting...")

        # Load market schedule once at startup
        logging.info("Loading initial market schedule...")
        if not self.load_market_schedule():
            logging.error("Failed to load initial market schedule, exiting")
            return

        while self.running:
            try:
                # Only reload market schedule when date changes or first time
                if self.needs_schedule_reload():
                    logging.info("Loading market schedule...")
                    if not self.load_market_schedule():
                        logging.error(f"Failed to load market schedule, sleeping {SCHEDULE_RELOAD_RETRY_HOURS} hour(s)")
                        time.sleep(SCHEDULE_RELOAD_RETRY_HOURS * 3600)
                        continue

                # Find next trading day
                next_date, _ = self.get_next_trading_day()
                if not next_date:
                    logging.info("No upcoming trading days found, sleeping 24 hours")
                    time.sleep(24 * 3600)
                    continue

                today = date.today()
                now = datetime.now()

                if next_date > today:
                    # Next trading day is in the future - sleep until 06:30 AM that day
                    next_start = datetime.combine(next_date, dt_time(6, 30))
                    sleep_seconds = int((next_start - now).total_seconds())
                    logging.info(f"Next trading day: {next_date}, sleeping {sleep_seconds} seconds until 06:30 AM")
                    time.sleep(sleep_seconds)
                    continue
                    
                elif next_date == today:
                    # Today is a trading day
                    start_time = dt_time(6, 30)  # 06:30 AM
                    force_kill_time = dt_time(13, 0, 10)  # 13:00:10 PM
                    
                    current_time = now.time()
                    
                    if current_time < start_time:
                        # Before market start
                        sleep_seconds = self.calculate_sleep_time(start_time)
                        logging.info(f"Sleeping {sleep_seconds} seconds until market start (06:30)")
                        time.sleep(sleep_seconds)
                        continue
                        
                    elif current_time < force_kill_time:
                        # During market hours
                        if not self.stream_running:
                            logging.info("Starting stream service...")
                            if self.start_service(STREAM_SERVICE_NAME):
                                self.stream_running = True
                                
                        if not self.monitor_running:
                            logging.info("Starting monitor service...")
                            if self.start_service(MONITOR_SERVICE_NAME):
                                self.monitor_running = True
                                
                        # Monitor services and restart if needed
                        if self.stream_running and not self.is_service_running(STREAM_SERVICE_NAME):
                            logging.warning("Stream service crashed, restarting...")
                            if self.start_service(STREAM_SERVICE_NAME):
                                self.stream_running = True
                            else:
                                self.stream_running = False
                                
                        if self.monitor_running and not self.is_service_running(MONITOR_SERVICE_NAME):
                            logging.warning("Monitor service crashed, restarting...")
                            if self.start_service(MONITOR_SERVICE_NAME):
                                self.monitor_running = True
                            else:
                                self.monitor_running = False
                                
                        # Sleep until force kill time or check interval
                        sleep_seconds = min(self.calculate_sleep_time(force_kill_time), SERVICE_CHECK_INTERVAL)
                        if sleep_seconds > 0:
                            time.sleep(sleep_seconds)
                        continue
                        
                    else:
                        # After force kill time
                        logging.info("Force kill time reached, stopping all services")
                        self._stop_all_services()

                        # Sleep until next day check time
                        tomorrow = today + timedelta(days=1)
                        check_time_parts = NEXT_DAY_CHECK_TIME.split(':')
                        check_hour = int(check_time_parts[0])
                        check_minute = int(check_time_parts[1])
                        next_check = datetime.combine(tomorrow, dt_time(check_hour, check_minute))
                        sleep_seconds = int((next_check - now).total_seconds())
                        logging.info(f"Sleeping {sleep_seconds} seconds until next day ({NEXT_DAY_CHECK_TIME})")
                        time.sleep(sleep_seconds)
                        continue
                        
            except Exception as e:
                logging.error(f"Controller error: {e}")
                time.sleep(ERROR_SLEEP_INTERVAL)  # Sleep on error
                
        logging.info("Controller shutting down")

if __name__ == "__main__":
    controller = SchwabStreamController()
    controller.run()
