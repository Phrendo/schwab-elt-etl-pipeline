"""
Schwab Option Chains Data Collection Service

This service fetches and stores option chains data from the Schwab API on a scheduled basis
during market hours. It collects SPX option chains with different frequencies and DTE ranges.

Features:
- Scheduled option chains data collection during market hours
- Multiple collection frequencies (1-minute, 5-minute, 30-minute)
- Different DTE (Days To Expiration) ranges for each frequency
- Database integration via tools.db module
- Configurable symbols, tables, and parameters via environment variables
- Market session awareness with padding
- Comprehensive logging and error handling

Dependencies:
- tools.db: Database operations
- tools.schwab: Schwab API client
- tools.utils: Date/time utility functions
- tools.logging_config: Logging configuration
"""

import time
import asyncio
import schedule
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv, find_dotenv

# Load environment variables with override to ensure fresh values
load_dotenv(find_dotenv(), override=True)

from tools.db import DB
from tools.schwab import SchwabAPI
from tools.utils import convert_epoch_to_pacific
from tools.logging_config import init_logging

# Initialize logging
logger = init_logging()

# Load configuration from environment variables
CHAINS_API_NAME = os.getenv("CHAINS_API_NAME", "MAIN_DATA")
CHAINS_SYMBOL = os.getenv("CHAINS_SYMBOL", "$SPX")
CHAINS_TABLE = os.getenv("CHAINS_TABLE", "CHAINS.dbo.SPX_CHAIN")
CHAINS_STRIKE_COUNT = int(os.getenv("CHAINS_STRIKE_COUNT", "200"))
CHAINS_SESSION_PADDING_SECONDS = int(os.getenv("CHAINS_SESSION_PADDING_SECONDS", "30"))

# DTE configuration for different frequencies
CHAINS_1MIN_DTE_FROM = int(os.getenv("CHAINS_1MIN_DTE_FROM", "0"))
CHAINS_1MIN_DTE_TO = int(os.getenv("CHAINS_1MIN_DTE_TO", "0"))
CHAINS_5MIN_DTE_FROM = int(os.getenv("CHAINS_5MIN_DTE_FROM", "0"))
CHAINS_5MIN_DTE_TO = int(os.getenv("CHAINS_5MIN_DTE_TO", "7"))
CHAINS_30MIN_DTE_FROM = int(os.getenv("CHAINS_30MIN_DTE_FROM", "8"))
CHAINS_30MIN_DTE_TO = int(os.getenv("CHAINS_30MIN_DTE_TO", "30"))

# Initialize database connection
db = DB()

def process_options(symbol: str, table: str, option_type_str: str, exp_date_map: dict):
    """
    Process option data and insert into database.

    Processes option chain data for calls or puts and stores them in the specified
    database table with proper data formatting and error handling.

    Args:
        symbol (str): The underlying symbol (e.g., '$SPX')
        table (str): Database table name to insert data into
        option_type_str (str): 'CALL' or 'PUT' to determine CP value
        exp_date_map (dict): Option data organized by expiration date and strike
    """
    cp_value = 1 if option_type_str == "CALL" else -1
    options_processed = 0

    for exp_date, strikes in exp_date_map.items():
        expiry_date = exp_date.split(":")[0]

        for strike_price, options in strikes.items():
            for option in options:
                try:
                    # Convert epoch timestamp to Pacific time
                    quote_time = option.get("quoteTimeInLong")
                    if quote_time:
                        # Convert from milliseconds to seconds for convert_epoch_to_pacific
                        dtime = convert_epoch_to_pacific(quote_time / 1000)
                    else:
                        dtime = None

                    weekly = 1 if option.get("optionRoot") == "SPXW" else 0

                    data = {
                        "CP": cp_value,
                        "Expiry": expiry_date,
                        "DTE": option.get("daysToExpiration"),
                        "Strike": int(float(strike_price)),
                        "Bid": option.get("bid"),
                        "Ask": option.get("ask"),
                        "Volume": option.get("totalVolume"),
                        "DTime": dtime,
                        "Volatility": option.get("volatility"),
                        "Delta": option.get("delta"),
                        "Gamma": option.get("gamma"),
                        "Theta": option.get("theta"),
                        "Vega": option.get("vega"),
                        "Rho": option.get("rho"),
                        "OI": option.get("openInterest"),
                        "Weekly": weekly
                    }

                    insert_query = f"""
                    INSERT INTO {table} (
                        CP, Expiry, DTE, Strike, Bid, Ask, Volume, DTime,
                        Volatility, Delta, Gamma, Theta, Vega, Rho, OI, Weekly
                    ) VALUES (
                        :CP, :Expiry, :DTE, :Strike, :Bid, :Ask, :Volume, :DTime,
                        :Volatility, :Delta, :Gamma, :Theta, :Vega, :Rho, :OI, :Weekly
                    )
                    """
                    db.execute_non_query(insert_query, data)
                    options_processed += 1

                except Exception as e:
                    logger.error(f"Error processing {option_type_str} option for {symbol} at strike {strike_price}",
                               extra={"error": str(e)})

    if options_processed > 0:
        logger.debug(f"Processed {options_processed} {option_type_str} options for {symbol}")

async def fetch_and_process(symbol: str, table: str, fromDTE: int, toDTE: int, strikeCount: int):
    """
    Fetch option chains data and process it into the database.

    Retrieves option chains from Schwab API for the specified parameters
    and processes both calls and puts into the database table.

    Args:
        symbol (str): The underlying symbol (e.g., '$SPX')
        table (str): Database table name to insert data into
        fromDTE (int): Starting days to expiration
        toDTE (int): Ending days to expiration
        strikeCount (int): Number of strikes to retrieve
    """
    schwab = SchwabAPI(CHAINS_API_NAME)
    logger.debug(f"Fetching option chains for {symbol} (DTE: {fromDTE}-{toDTE}, Strikes: {strikeCount})")

    try:
        response = await schwab.get_chains(symbol=symbol, fromDTE=fromDTE, toDTE=toDTE, strikeCount=strikeCount)

        # Check for API errors
        if response and isinstance(response, dict) and "fault" in response:
            fault = response["fault"]
            if fault.get("faultstring") == "Body buffer overflow":
                logger.error(f"Body buffer overflow detected for {symbol}", extra={"fault": fault})
                return
            else:
                logger.error(f"API fault for {symbol}", extra={"fault": fault})
                return

        if not response:
            logger.error(f"No option chains data received for {symbol}")
            return

        calls_processed = puts_processed = 0

        # Process call options
        if "callExpDateMap" in response:
            calls_count_before = db.execute_query(f"SELECT COUNT(*) as count FROM {table} WHERE CP = 1", {})
            process_options(symbol, table, "CALL", response["callExpDateMap"])
            calls_count_after = db.execute_query(f"SELECT COUNT(*) as count FROM {table} WHERE CP = 1", {})
            calls_processed = calls_count_after[0].count - calls_count_before[0].count if calls_count_before and calls_count_after else 0

        # Process put options
        if "putExpDateMap" in response:
            puts_count_before = db.execute_query(f"SELECT COUNT(*) as count FROM {table} WHERE CP = -1", {})
            process_options(symbol, table, "PUT", response["putExpDateMap"])
            puts_count_after = db.execute_query(f"SELECT COUNT(*) as count FROM {table} WHERE CP = -1", {})
            puts_processed = puts_count_after[0].count - puts_count_before[0].count if puts_count_before and puts_count_after else 0

        logger.info(f"✅ Processed {symbol} chains: {calls_processed} calls, {puts_processed} puts (DTE: {fromDTE}-{toDTE})")

    except Exception as e:
        logger.error(f"Failed to fetch/process option chains for {symbol}", extra={"error": str(e)})

def scheduled_job():
    """
    Execute scheduled option chains collection based on market session and time.

    Runs different collection jobs based on the current time:
    - Every minute: Current day options (DTE 0-0)
    - Every 5 minutes: Near-term options (DTE 0-7)
    - Every 30 minutes: Longer-term options (DTE 8-30)
    """
    current_time = datetime.now()
    session = db.get_today_session()

    if not session or not session.get('is_open'):
        logger.debug("Market is closed, skipping option chains collection")
        return

    # Add configured padding to the session start and end times
    padding = timedelta(seconds=CHAINS_SESSION_PADDING_SECONDS)
    padded_session_start = session['session_start'] - padding
    padded_session_end = session['session_end'] + padding

    if not (padded_session_start <= current_time <= padded_session_end):
        logger.debug("Outside padded market hours, skipping option chains collection")
        return

    try:
        # 1-minute job: Current day options
        logger.info("Running 1-minute option chains collection")
        asyncio.run(fetch_and_process(
            CHAINS_SYMBOL,
            CHAINS_TABLE,
            CHAINS_1MIN_DTE_FROM,
            CHAINS_1MIN_DTE_TO,
            CHAINS_STRIKE_COUNT
        ))

        # 5-minute job: Near-term options
        if current_time.minute % 5 == 0:
            logger.info("Running 5-minute option chains collection")
            asyncio.run(fetch_and_process(
                CHAINS_SYMBOL,
                CHAINS_TABLE,
                CHAINS_5MIN_DTE_FROM,
                CHAINS_5MIN_DTE_TO,
                CHAINS_STRIKE_COUNT
            ))

        # 30-minute job: Longer-term options
        if current_time.minute in [0, 30]:
            logger.info("Running 30-minute option chains collection")
            asyncio.run(fetch_and_process(
                CHAINS_SYMBOL,
                CHAINS_TABLE,
                CHAINS_30MIN_DTE_FROM,
                CHAINS_30MIN_DTE_TO,
                CHAINS_STRIKE_COUNT
            ))

    except Exception as e:
        logger.error("Error in scheduled option chains job", extra={"error": str(e)})


def main():
    """
    Main entry point for the option chains service.

    Sets up scheduling and runs continuously to collect option chains data.
    """
    logger.info("Starting Schwab Option Chains Data Collection Service")
    logger.info(f"Using API: {CHAINS_API_NAME}")
    logger.info(f"Symbol: {CHAINS_SYMBOL}")
    logger.info(f"Table: {CHAINS_TABLE}")
    logger.info(f"Strike count: {CHAINS_STRIKE_COUNT}")
    logger.info(f"Session padding: {CHAINS_SESSION_PADDING_SECONDS} seconds")
    logger.info(f"DTE ranges - 1min: {CHAINS_1MIN_DTE_FROM}-{CHAINS_1MIN_DTE_TO}, "
               f"5min: {CHAINS_5MIN_DTE_FROM}-{CHAINS_5MIN_DTE_TO}, "
               f"30min: {CHAINS_30MIN_DTE_FROM}-{CHAINS_30MIN_DTE_TO}")

    # Schedule the job to run every minute
    schedule.every().minute.at(":00").do(scheduled_job)
    logger.info("Scheduled option chains collection every minute during market hours")

    # Run initial collection
    logger.info("Running initial option chains collection...")
    scheduled_job()

    # Main service loop
    logger.info("Entering main service loop...")
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)  # Check every second for precise timing
        except KeyboardInterrupt:
            logger.info("Service interrupted by user")
            break
        except Exception as e:
            logger.error("Unexpected error in main loop", extra={"error": str(e)})
            time.sleep(1)  # Continue after error

    logger.info("Option chains service stopped")


if __name__ == "__main__":
    main()