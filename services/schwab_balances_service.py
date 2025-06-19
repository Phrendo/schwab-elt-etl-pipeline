import asyncio
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from tools.db import DB
from tools.schwab import SchwabAPI
from tools.config import get_config

"""
Schwab Account Balance Service

This service fetches account balance data from the Schwab API at scheduled intervals
and stores it in the database for tracking and analysis.

Configuration via environment variables:
- TRADE_API_NAME: API name for trading operations (default: MAIN_TRADE)
- BALANCE_CHECK_TIMES: Comma-separated list of times to check balances (default: 06:00,10:00,14:00,14:45,18:00)
- TIMEZONE: Timezone for scheduling and timestamps (default: US/Pacific)
- BALANCE_CHECK_INTERVAL: Seconds between schedule checks (default: 10)
- BALANCE_COOLDOWN: Seconds to wait after running to prevent duplicates (default: 60)

systemd service file: sudo nano /etc/systemd/system/schwab_balances_service.service
"""

# Load configuration from centralized config system
config = get_config()
app_config = config.get_service_config('application')
api_config = app_config.get('api', {})
balance_config = config.get_service_config('balance_service')

TRADE_API_NAME = api_config.get('trade_name', 'MAIN_TRADE')
BALANCE_CHECK_TIMES = balance_config.get('check_times', ['06:00', '10:00', '14:00', '14:45', '18:00'])
TIMEZONE = app_config.get('timezone', 'US/Pacific')
BALANCE_CHECK_INTERVAL = balance_config.get('check_interval', 10)  # seconds
BALANCE_COOLDOWN = balance_config.get('cooldown', 60)  # seconds

async def get_balances():
    """
    Fetch and store account balance data.

    Retrieves current account balance information from the Schwab API
    and stores it in the database with timestamp and account details.
    """
    print(f"Fetching Schwab account balance using API: {TRADE_API_NAME}")

    try:
        # Initialize database and API connections
        db = DB()
        schwab = SchwabAPI(TRADE_API_NAME)
        account_data = await schwab.get_account_balance()

        if not account_data:
            print("Failed to retrieve account balance.")
            return

        account_info = account_data.get("securitiesAccount", {})
        balance_info = account_info.get("currentBalances", {})

        # Get current time in configured timezone, remove microseconds for DATETIME2(0)
        proc_time = datetime.now(ZoneInfo(TIMEZONE)).replace(microsecond=0)
        proc_time_naive = proc_time.replace(tzinfo=None)

        balance_data = {
            "ApiCallTime": proc_time_naive,  # Stored in configured timezone as DATETIME2(0)
            "accountId": int(account_info.get("accountNumber")),
            "roundTrips": account_info.get("roundTrips"),
            "isDayTrader": int(account_info.get("isDayTrader")),
            "isClosingOnly": int(account_info.get("isClosingOnlyRestricted")),
            "buyingPower": float(balance_info.get("dayTradingBuyingPower")),
            "cashBalance": float(balance_info.get("cashBalance")),
            "liquidationValue": float(balance_info.get("liquidationValue")),
        }

        db.insert_balances(balance_data)
        print(f"Balance data inserted successfully at {proc_time_naive}")

    except Exception as e:
        print(f"Error fetching or storing balance data: {str(e)}")
        # Re-raise to allow calling code to handle appropriately
        raise

async def scheduler():
    """
    Schedules balance checks at specific times.

    Continuously monitors the current time and triggers balance checks
    when the time matches any of the configured check times.
    """
    loop = asyncio.get_running_loop()

    # Clean up the configured times (remove whitespace)
    check_times = [time.strip() for time in BALANCE_CHECK_TIMES]

    print(f"Scheduler started. Running balance checks at: {', '.join(check_times)}")
    print(f"Timezone: {TIMEZONE}")
    print(f"Check interval: {BALANCE_CHECK_INTERVAL} seconds")
    print(f"Cooldown period: {BALANCE_COOLDOWN} seconds")

    while True:
        # Get current time in the configured timezone
        now = datetime.now(ZoneInfo(TIMEZONE)).strftime("%H:%M")

        if now in check_times:
            print(f"Triggering balance check at {now}")
            try:
                # Create task to run balance check asynchronously
                loop.create_task(get_balances())
                # Wait cooldown period to prevent multiple runs in the same minute
                await asyncio.sleep(BALANCE_COOLDOWN)
            except Exception as e:
                print(f"Error during scheduled balance check: {str(e)}")
                # Continue running even if one check fails
                await asyncio.sleep(BALANCE_COOLDOWN)

        # Check every configured interval
        await asyncio.sleep(BALANCE_CHECK_INTERVAL)

async def main():
    """
    Main entry point for the balance service.

    Starts the scheduler which will run balance checks at configured times.
    """
    print("=== Schwab Account Balance Service ===")
    print(f"Configuration:")
    print(f"  API Name: {TRADE_API_NAME}")
    print(f"  Check Times: {', '.join(BALANCE_CHECK_TIMES)}")
    print(f"  Timezone: {TIMEZONE}")
    print(f"  Check Interval: {BALANCE_CHECK_INTERVAL}s")
    print(f"  Cooldown: {BALANCE_COOLDOWN}s")
    print("=" * 40)

    try:
        await scheduler()
    except KeyboardInterrupt:
        print("\nService stopped by user.")
    except Exception as e:
        print(f"Service error: {str(e)}")
        raise

def run_once():
    """
    Run a single balance check (useful for testing or manual execution).
    """
    print("Running single balance check...")
    asyncio.run(get_balances())

def run_scheduler():
    """
    Run the continuous scheduler (normal service operation).
    """
    asyncio.run(main())

if __name__ == "__main__":
    import sys

    # Allow running in different modes based on command line arguments
    if len(sys.argv) > 1 and sys.argv[1] == "once":
        run_once()
    else:
        run_scheduler()
