"""
Schwab Market Hours Service

This service fetches and stores market hours data from the Schwab API on a scheduled basis.
It monitors equity market hours and updates the database with session times and market status.

Features:
- Scheduled market hours data collection
- Database integration via tools.db module
- Configurable check times via environment variables
- Pacific Time timezone handling
- Comprehensive logging and error handling

Dependencies:
- tools.db: Database operations
- tools.schwab: Schwab API client
- tools.utils: Date/time utility functions
- tools.logging_config: Logging configuration
"""

import asyncio
import schedule
import time
import os
from datetime import datetime, date
from zoneinfo import ZoneInfo
from dotenv import load_dotenv, find_dotenv

# Load environment variables with override to ensure fresh values
load_dotenv(find_dotenv(), override=True)

from tools.db import DB
from tools.schwab import SchwabAPI
from tools.utils import convert_to_pacific_time, parse_date
from tools.logging_config import init_logging

# Initialize logging
logger = init_logging()

# Load configuration from environment variables
MARKET_HOURS_CHECK_TIMES = os.getenv("MARKET_HOURS_CHECK_TIMES", "06:28,07:30,02:00").split(",")
TIMEZONE = os.getenv("TIMEZONE", "US/Pacific")
MARKET_HOURS_CHECK_INTERVAL = int(os.getenv("MARKET_HOURS_CHECK_INTERVAL", "60"))  # seconds
TRADE_API_NAME = os.getenv("TRADE_API_NAME", "MAIN_TRADE")

# Initialize DB and API clients
db = DB()
client = SchwabAPI(TRADE_API_NAME)

def upsert_market_hours_for_today():
    """
    Fetch and upsert market hours for the current date.

    Retrieves market hours data from the Schwab API and stores it in the database
    using the configured timezone and database methods.
    """
    market_date = date.today()
    logger.info(f"Fetching market hours for {market_date}")

    # Fetch market hours data from Schwab API
    try:
        response = asyncio.run(client.get_markets('equity', market_date))
    except Exception as e:
        logger.error("Failed to fetch market data from Schwab API", extra={"error": str(e)})
        return

    # Get current processing time in configured timezone
    proc_time_pacific = datetime.now(ZoneInfo(TIMEZONE))

    if response and 'equity' in response:
        # Parse equity market data from API response
        equity_data = response['equity'].get('EQ') or response['equity'].get('equity', {})
        is_open = equity_data.get('isOpen', False)
        market_type = equity_data.get('marketType', 'UNKNOWN')

        session_hours = equity_data.get('sessionHours', {})
        regular_sessions = session_hours.get('regularMarket', [])

        # Default to None for closed days
        session_start_time = None
        session_end_time = None

        if is_open and regular_sessions:
            session = regular_sessions[0]
            session_start = session.get('start')
            session_end = session.get('end')

            if session_start and session_end:
                # Convert to Pacific Time and extract only the time part
                session_start_dt = convert_to_pacific_time(parse_date(session_start))
                session_end_dt = convert_to_pacific_time(parse_date(session_end))

                # Extract 'HH:MM:SS' for TIME(0) database column
                session_start_time = session_start_dt.split(' ')[1].split('.')[0]
                session_end_time = session_end_dt.split(' ')[1].split('.')[0]

        # Prepare market hours data for database
        market_hours_data = {
            "ProcTime": proc_time_pacific,
            "market_date": market_date,
            "market_type": market_type,
            "session_start": session_start_time,
            "session_end": session_end_time,
            "is_open": int(is_open)
        }

        try:
            # Use the new database method for upserting market hours
            db.upsert_market_hours(market_hours_data)
            logger.info(f"[{market_date}] Market hours upsert successful - Open: {is_open}, Type: {market_type}")

            if session_start_time and session_end_time:
                logger.info(f"[{market_date}] Session hours: {session_start_time} - {session_end_time}")

        except Exception as e:
            logger.error("Failed to upsert market hours to database", extra={"error": str(e)})

    else:
        logger.error(f"[{market_date}] No valid market data received from API", extra={"response": str(response)})

def main():
    """
    Main service loop for market hours monitoring.

    Schedules market hours checks at configured times and runs continuously
    to monitor and update market hours data.
    """
    logger.info("Starting Schwab Market Hours Service")
    logger.info(f"Using API: {TRADE_API_NAME}")
    logger.info(f"Timezone: {TIMEZONE}")
    logger.info(f"Check interval: {MARKET_HOURS_CHECK_INTERVAL} seconds")

    # Schedule market hours checks at configured times
    for check_time in MARKET_HOURS_CHECK_TIMES:
        check_time = check_time.strip()  # Remove any whitespace
        schedule.every().day.at(check_time).do(upsert_market_hours_for_today)
        logger.info(f"Scheduled market hours check at {check_time}")

    logger.info("Market hours service started successfully")

    # Run immediately on startup
    logger.info("Running initial market hours check...")
    upsert_market_hours_for_today()

    # Continuous loop to keep the service running
    logger.info("Entering main service loop...")
    while True:
        try:
            schedule.run_pending()
            time.sleep(MARKET_HOURS_CHECK_INTERVAL)
        except KeyboardInterrupt:
            logger.info("Service interrupted by user")
            break
        except Exception as e:
            logger.error("Unexpected error in main loop", extra={"error": str(e)})
            time.sleep(MARKET_HOURS_CHECK_INTERVAL)  # Continue after error

    logger.info("Market hours service stopped")

if __name__ == "__main__":
    main()
