"""
Schwab OHLC Data Collection Service

This service fetches and stores historical OHLC (Open, High, Low, Close) data
from the Schwab API on a scheduled basis for specified symbols.

Features:
- Scheduled OHLC data collection for multiple symbols
- Both minute-level and daily data collection
- Database integration via tools.db module
- Configurable symbols and schedule times via environment variables
- Stored procedure execution for data processing
- Comprehensive logging and error handling

Dependencies:
- tools.db: Database operations
- tools.schwab: Schwab API client
- tools.logging_config: Logging configuration
"""

import asyncio
import schedule
import time
import os
from dotenv import load_dotenv, find_dotenv

from tools.db import DB
from tools.schwab import SchwabAPI
from tools.logging_config import init_logging
from tools.config import get_config

# Initialize logging
logger = init_logging()

# Load configuration from centralized config system
config = get_config()
ohlc_config = config.get_service_config('ohlc_service')

OHLC_API_NAME = ohlc_config.get("api_name", "MAIN_DATA")
OHLC_SYMBOLS = ohlc_config.get("symbols", ["$SPX", "$VIX"])
OHLC_WEEKDAY_TIMES = ohlc_config.get("weekday_times", ["06:15", "13:15"])
OHLC_SATURDAY_TIMES = ohlc_config.get("saturday_times", ["06:15"])
OHLC_CHECK_INTERVAL = ohlc_config.get("check_interval", 60)  # seconds
OHLC_STORED_PROCEDURE = ohlc_config.get("stored_procedure", "PYTHON.SP_PY_PROCESS_OHLC")

# Initialize database and API connection
db = DB()
schwab = SchwabAPI(OHLC_API_NAME)

async def run_daily_task():
    """
    Run the daily OHLC data collection task.

    Fetches both minute-level and daily OHLC data for all configured symbols
    and processes the data through the configured stored procedure.
    """
    logger.info("Starting daily OHLC data collection...")
    logger.info(f"Using API: {OHLC_API_NAME}")
    logger.info(f"Symbols: {', '.join(OHLC_SYMBOLS)}")

    try:
        # Ensure tokens are up to date
        await schwab.token_handler()

        # Fetch minute-level OHLC data for all symbols
        logger.info("Fetching minute-level OHLC data...")
        for symbol in OHLC_SYMBOLS:
            symbol = symbol.strip()  # Remove any whitespace
            try:
                await schwab.get_historic_quote_to_sql_minute(symbol)
            except Exception as e:
                logger.error(f"Failed to fetch minute data for {symbol}", extra={"error": str(e)})

        # Fetch daily OHLC data for all symbols
        logger.info("Fetching daily OHLC data...")
        for symbol in OHLC_SYMBOLS:
            symbol = symbol.strip()  # Remove any whitespace
            try:
                await schwab.get_historic_quote_to_sql_day(symbol)
            except Exception as e:
                logger.error(f"Failed to fetch daily data for {symbol}", extra={"error": str(e)})

        # Process staging table using stored procedure
        logger.info(f"Processing data with stored procedure: {OHLC_STORED_PROCEDURE}")
        try:
            db.execute_stored_procedure(OHLC_STORED_PROCEDURE)
            logger.info("Stored procedure executed successfully")
        except Exception as e:
            logger.error("Failed to execute stored procedure", extra={"error": str(e)})
            raise

        logger.info("Daily OHLC collection completed successfully")

    except Exception as e:
        logger.error("Daily OHLC collection failed", extra={"error": str(e)})
        raise

def run_async_task():
    """
    Helper function to run the async task in the current event loop.

    Wraps the async run_daily_task() function for use with the schedule library.
    """
    try:
        asyncio.run(run_daily_task())
    except Exception as e:
        logger.error("Error in async task execution", extra={"error": str(e)})


def setup_schedule():
    """
    Set up the OHLC data collection schedule based on configuration.

    Schedules tasks for weekdays and Saturday based on environment variables.
    """
    logger.info("Setting up OHLC data collection schedule...")

    # Schedule weekday tasks (Monday-Friday)
    weekdays = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']
    for day in weekdays:
        for check_time in OHLC_WEEKDAY_TIMES:
            check_time = check_time.strip()  # Remove any whitespace
            getattr(schedule.every(), day).at(check_time).do(run_async_task)
            logger.info(f"Scheduled OHLC collection on {day.capitalize()} at {check_time}")

    # Schedule Saturday tasks
    for check_time in OHLC_SATURDAY_TIMES:
        check_time = check_time.strip()  # Remove any whitespace
        schedule.every().saturday.at(check_time).do(run_async_task)
        logger.info(f"Scheduled OHLC collection on Saturday at {check_time}")


def main():
    """
    Main service loop for OHLC data collection.

    Sets up scheduling and runs continuously to monitor and collect OHLC data.
    """
    logger.info("Starting Schwab OHLC Data Collection Service")
    logger.info(f"Using API: {OHLC_API_NAME}")
    logger.info(f"Symbols: {', '.join(OHLC_SYMBOLS)}")
    logger.info(f"Check interval: {OHLC_CHECK_INTERVAL} seconds")
    logger.info(f"Stored procedure: {OHLC_STORED_PROCEDURE}")

    # Set up the schedule
    setup_schedule()

    logger.info("OHLC service started successfully")

    # Run immediately on startup
    logger.info("Running initial OHLC data collection...")
    run_async_task()

    # Continuous loop to keep the service running
    logger.info("Entering main service loop...")
    while True:
        try:
            schedule.run_pending()
            time.sleep(OHLC_CHECK_INTERVAL)
        except KeyboardInterrupt:
            logger.info("Service interrupted by user")
            break
        except Exception as e:
            logger.error("Unexpected error in main loop", extra={"error": str(e)})
            time.sleep(OHLC_CHECK_INTERVAL)  # Continue after error

    logger.info("OHLC service stopped")


if __name__ == "__main__":
    main()
