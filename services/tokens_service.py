#!/usr/bin/env python3
"""
token_monitor.py

Continuously watches two Schwab API tokens and refreshes whichever is expiring next, 
with a safety margin.

    NOTE_001: Schwab allows for two API names (one for trade/accounts and one for data),
    the code below handles both cases. If you have only one API name configured,
    set DATA_API_NAME and TRADE_API_NAME to the same value in .env.

Environment variables:
  REFRESH_THRESHOLD  seconds before actual expiry to trigger a refresh
  DATA_API_NAME      name of the data API instance in the database
  TRADE_API_NAME     name of the trade API instance in the database
"""

from tools.schwab import SchwabAPI
from tools.config import get_config
from datetime import datetime
import time, asyncio, os

# Load configuration from centralized config system
config = get_config()
tokens_config = config.get_service_config('tokens')
app_config = config.get_service_config('application')
api_config = app_config.get('api', {})

REFRESH_THRESHOLD = tokens_config.get('refresh_threshold', 60)
DATA_API_NAME = api_config.get('data_name', 'MAIN_DATA')
TRADE_API_NAME = api_config.get('trade_name', 'MAIN_TRADE')
SINGLE_API_MODE = DATA_API_NAME == TRADE_API_NAME # Determine if we're using a single API for both data and trade see NOTE_001

async def monitor_tokens(refresh_threshold=REFRESH_THRESHOLD):
    """
    Loop forever, checking the expiration times of both API tokens and
    proactively refreshing them {REFRESH_THRESHOLD} seconds before they expire.
    
    Args:
        refresh_threshold (int): Number of seconds before expiry to refresh.
    """
    api_data = SchwabAPI(DATA_API_NAME)
    api_trade = None if SINGLE_API_MODE else SchwabAPI(TRADE_API_NAME)

    while True:
        current_time = time.time()
        current_time_str = datetime.fromtimestamp(current_time).strftime('%H:%M:%S')

        # Get token expiration times
        data_exp = api_data.tokens.get('access_token_expires_at', float('inf'))
        trade_exp = data_exp if SINGLE_API_MODE else api_trade.tokens.get('access_token_expires_at', float('inf'))

        # Convert expiration times to HH:MM:SS format
        data_time = datetime.fromtimestamp(data_exp).strftime('%H:%M:%S') if data_exp != float('inf') else "N/A"
        trade_time = datetime.fromtimestamp(trade_exp).strftime('%H:%M:%S') if trade_exp != float('inf') else "N/A"

        # Determine which token expires earlier
        next_expiration = min(data_exp, trade_exp)

        # Calculate time until next refresh
        time_until_refresh = next_expiration - time.time() - refresh_threshold

        if time_until_refresh <= 0:
            if data_exp >= trade_exp:
                await api_data.token_handler()
                await api_trade.token_handler() if not SINGLE_API_MODE else None
            else:
                await api_trade.token_handler() if not SINGLE_API_MODE else None
                await api_data.token_handler()

            # Recalculate new expiration times after refresh
            data_exp = api_data.tokens.get('access_token_expires_at', float('inf'))
            trade_exp = data_exp if SINGLE_API_MODE else api_trade.tokens.get('access_token_expires_at', float('inf'))
            next_expiration = min(data_exp, trade_exp)

            data_time = datetime.fromtimestamp(data_exp).strftime('%H:%M:%S') if data_exp != float('inf') else "N/A"
            trade_time = datetime.fromtimestamp(trade_exp).strftime('%H:%M:%S') if trade_exp != float('inf') else "N/A"

            # Calculate new wait time
            time_until_refresh = next_expiration - time.time() - refresh_threshold

        # Min 10 seconds to prevent tight loops
        wait_time = max(10, time_until_refresh)

        next_refresh_time_str = datetime.fromtimestamp(time.time() + wait_time).strftime('%H:%M:%S')

        # This can be either a print or log, on the back, we utilize journalctl so both work fine as this is systemctl managed
        print(f"⏳ TIME: {current_time_str} | NEXT REFRESH: {next_refresh_time_str} | {DATA_API_NAME}: {data_time} | {TRADE_API_NAME}: {trade_time}")

        await asyncio.sleep(wait_time)

async def main():
    await monitor_tokens()

if __name__ == "__main__":
    asyncio.run(main())
