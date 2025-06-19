"""
Schwab Account Hash Retrieval Script

This script retrieves and stores the account hash from the Schwab API.
The account hash is required for account-specific API operations like
getting balances, placing orders, etc.

This is typically only needed to be run once, unless:
- The API app is reset
- Account access is changed
- The hash becomes invalid

The hash is stored in the SCHWAB.HASH table and used by other services
that need to make account-specific API calls.
"""

import asyncio
import os
from datetime import datetime
from tools.db import DB
from tools.schwab import SchwabAPI
from tools.config import get_config

# Load configuration from centralized config system
config = get_config()
app_config = config.get_service_config('application')
api_config = app_config.get('api', {})

TRADE_API_NAME = api_config.get('trade_name', 'MAIN_TRADE')
ACCOUNT_NUMBER = config.get_secret('ACCNT_NUM')

if not ACCOUNT_NUMBER:
    print("Error: ACCNT_NUM environment variable is required")
    exit(1)

async def main():
    """
    Main function to retrieve and store account hash.

    This function:
    1. Creates a SchwabAPI instance
    2. Retrieves account information from Schwab API
    3. Extracts the account hash for the configured account number
    4. Stores the hash in the database for future use
    """
    print("=== Schwab Account Hash Retrieval ===")
    print(f"API Name: {TRADE_API_NAME}")
    print(f"Account Number: {ACCOUNT_NUMBER}")
    print("=" * 40)

    try:
        # Create an instance of SchwabAPI
        schwab = SchwabAPI(TRADE_API_NAME)

        # Retrieve the accounts from the Schwab API
        print("Fetching account information from Schwab API...")
        accounts = await schwab.get_accounts()
        if not accounts:
            print("Error: Could not retrieve accounts from Schwab API.")
            return False

        print(f"Retrieved {len(accounts)} account(s) from API")

        # Extract the account hash using the existing get_account_hash method
        account_hash = schwab.get_account_hash(accounts, ACCOUNT_NUMBER)
        if not account_hash:
            print(f"Error: Account hash not found for account number {ACCOUNT_NUMBER}.")
            print("Available accounts:")
            for account in accounts:
                print(f"  - Account Number: {account.get('accountNumber')}")
            return False

        print(f"Found account hash: {account_hash[:8]}...")

        # Prepare the hash data for database storage
        hash_data = {
            'name': schwab.name,
            'account_number': ACCOUNT_NUMBER,
            'account_hash': account_hash,
            'update_time': datetime.now()
        }

        # Insert/update the record in the database using db.py method
        print("Storing account hash in database...")
        db = DB()
        db.insert_account_hash(hash_data)
        print("✅ Account hash stored successfully!")
        print(f"API Name: {schwab.name}")
        print(f"Account Number: {ACCOUNT_NUMBER}")
        print(f"Hash: {account_hash[:8]}...{account_hash[-8:]}")
        print(f"Updated: {hash_data['update_time']}")

        return True

    except Exception as e:
        print(f"❌ Error during hash retrieval: {str(e)}")
        return False

def run_hash_retrieval():
    """
    Run the hash retrieval process.

    This is the main entry point for the script. It runs the async main()
    function and handles the result.
    """
    print(f"Starting hash retrieval at {datetime.now()}")
    success = asyncio.run(main())

    if success:
        print("\n🎉 Hash retrieval completed successfully!")
        print("The account hash is now stored in the database and can be used by other services.")
    else:
        print("\n💥 Hash retrieval failed!")
        print("Please check the error messages above and try again.")
        exit(1)

if __name__ == "__main__":
    run_hash_retrieval()
