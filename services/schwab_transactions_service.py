"""
Schwab Transactions Processing Service

This service fetches and processes transaction data from the Schwab API on a continuous basis
during market hours. It monitors orders and executions, storing both raw JSON and structured data.

Features:
- Continuous transaction monitoring during market hours
- Raw JSON storage for complete audit trail
- Structured data processing for analysis
- Database integration via tools.db module
- Configurable market hours and intervals via environment variables
- Stored procedure execution for data processing
- Comprehensive logging and error handling

Dependencies:
- tools.db: Database operations
- tools.schwab: Schwab API client
- tools.logging_config: Logging configuration
"""

import asyncio
import time
import os
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv, find_dotenv

# Load environment variables with override to ensure fresh values
load_dotenv(find_dotenv(), override=True)

from tools.db import DB
from tools.schwab import SchwabAPI
from tools.logging_config import init_logging

# Initialize logging
logger = init_logging()

# Load configuration from environment variables
TRANSACTIONS_API_NAME = os.getenv("TRANSACTIONS_API_NAME", "MAIN_TRADE")
ACCNT_NUM = os.getenv("ACCNT_NUM")  # Account number from .env
TRANSACTIONS_MARKET_START_HOUR = int(os.getenv("TRANSACTIONS_MARKET_START_HOUR", "6"))
TRANSACTIONS_MARKET_START_MINUTE = int(os.getenv("TRANSACTIONS_MARKET_START_MINUTE", "30"))
TRANSACTIONS_MARKET_END_HOUR = int(os.getenv("TRANSACTIONS_MARKET_END_HOUR", "13"))
TRANSACTIONS_MARKET_END_MINUTE = int(os.getenv("TRANSACTIONS_MARKET_END_MINUTE", "0"))
TRANSACTIONS_ACTIVE_INTERVAL = int(os.getenv("TRANSACTIONS_ACTIVE_INTERVAL", "10"))  # seconds during market hours
TRANSACTIONS_INACTIVE_INTERVAL = int(os.getenv("TRANSACTIONS_INACTIVE_INTERVAL", "60"))  # seconds outside market hours
TRANSACTIONS_STORED_PROCEDURE = os.getenv("TRANSACTIONS_STORED_PROCEDURE", "OPT.PYTHON.SP_PY_PARSE_TRANSACTIONS")
TRANSACTIONS_NO_ORDERS_LOG_INTERVAL = int(os.getenv("TRANSACTIONS_NO_ORDERS_LOG_INTERVAL", "60"))  # seconds

# Global variable to track when we last logged "no orders received"
last_no_orders_print_time = 0

async def run_transaction_processing():
    """
    Fetch and process Schwab transactions.

    Retrieves orders from the last 24 hours, stores raw JSON data,
    processes structured data, and executes the stored procedure.
    """
    global last_no_orders_print_time
    current_time = time.time()

    try:
        # Initialize database and API connections
        db = DB()
        schwab = SchwabAPI(TRANSACTIONS_API_NAME)

        # Use the cached account hash loaded during initialization
        account_hash = schwab.account_hash

        if not account_hash:
            logger.error("Unable to retrieve account hash for transaction processing")
            return

        # Prepare date range for the last 24 hours
        now = datetime.now(timezone.utc)
        end_date = now.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + "Z"
        start_date = (now - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + "Z"

        logger.debug(f"Fetching orders from {start_date} to {end_date}")

        # Fetch orders from Schwab API
        orders_dump = await schwab.get_orders(account_hash, start_date, end_date)

        if not orders_dump:
            # Log "no orders" message at most once per configured interval
            if (current_time - last_no_orders_print_time > TRANSACTIONS_NO_ORDERS_LOG_INTERVAL):
                logger.info("No orders retrieved from Schwab API")
                last_no_orders_print_time = current_time
            return

        logger.info(f"Processing {len(orders_dump)} orders")

        # Process each order
        orders_processed = 0
        for order in orders_dump:
            try:
                # Store raw JSON data
                db.insert_raw_json(order)

                # Process structured order data
                db.process_order(order)
                orders_processed += 1

            except Exception as e:
                order_id = order.get('orderId', 'UNKNOWN')
                logger.error(f"Failed to process order {order_id}", extra={"error": str(e)})

        # Execute stored procedure to process the data
        try:
            db.execute_stored_procedure(TRANSACTIONS_STORED_PROCEDURE)
            logger.info(f"✅ Processed {orders_processed} transactions and executed stored procedure at {datetime.now().strftime('%H:%M:%S')}")
        except Exception as e:
            logger.error("Failed to execute transactions stored procedure", extra={"error": str(e)})

    except Exception as e:
        logger.error("Error in transaction processing", extra={"error": str(e)})

def is_market_hours():
    """
    Check if current time is within configured market hours on a weekday.

    Returns:
        bool: True if current time is during market hours on a weekday
    """
    now = datetime.now()

    # Check if it's a weekday (Monday=0, Sunday=6)
    if now.weekday() >= 5:  # Saturday or Sunday
        return False

    # Create market open and close times for today
    market_open = now.replace(
        hour=TRANSACTIONS_MARKET_START_HOUR,
        minute=TRANSACTIONS_MARKET_START_MINUTE,
        second=0,
        microsecond=0
    )
    market_close = now.replace(
        hour=TRANSACTIONS_MARKET_END_HOUR,
        minute=TRANSACTIONS_MARKET_END_MINUTE,
        second=0,
        microsecond=0
    )

    return market_open <= now <= market_close


async def scheduler():
    """
    Run transaction processing with different intervals based on market hours.

    Processes transactions every 10 seconds during market hours (weekdays 6:30 AM - 1:00 PM)
    and every 60 seconds outside market hours.
    """
    logger.info("Starting transaction processing scheduler")
    logger.info(f"Market hours: {TRANSACTIONS_MARKET_START_HOUR:02d}:{TRANSACTIONS_MARKET_START_MINUTE:02d} - {TRANSACTIONS_MARKET_END_HOUR:02d}:{TRANSACTIONS_MARKET_END_MINUTE:02d} (weekdays)")
    logger.info(f"Active interval: {TRANSACTIONS_ACTIVE_INTERVAL}s, Inactive interval: {TRANSACTIONS_INACTIVE_INTERVAL}s")

    while True:
        try:
            if is_market_hours():
                # During market hours - process frequently
                await run_transaction_processing()
                await asyncio.sleep(TRANSACTIONS_ACTIVE_INTERVAL)
            else:
                # Outside market hours - process less frequently
                await run_transaction_processing()
                await asyncio.sleep(TRANSACTIONS_INACTIVE_INTERVAL)

        except KeyboardInterrupt:
            logger.info("Scheduler interrupted by user")
            break
        except Exception as e:
            logger.error("Unexpected error in scheduler", extra={"error": str(e)})
            await asyncio.sleep(TRANSACTIONS_INACTIVE_INTERVAL)  # Use longer interval on error


async def main():
    """
    Main entry point for the transactions service.

    Initializes the service and starts the continuous scheduler.
    """
    logger.info("Starting Schwab Transactions Processing Service")
    logger.info(f"Using API: {TRANSACTIONS_API_NAME}")
    logger.info(f"Account: {ACCNT_NUM}")
    logger.info(f"Stored procedure: {TRANSACTIONS_STORED_PROCEDURE}")

    # Run initial transaction processing
    logger.info("Running initial transaction processing...")
    await run_transaction_processing()

    # Start the continuous scheduler
    logger.info("Starting continuous transaction monitoring...")
    await scheduler()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Transactions service stopped by user")
    except Exception as e:
        logger.error("Fatal error in transactions service", extra={"error": str(e)})
