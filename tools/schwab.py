"""
Unified Schwab API Client Module

This module provides a comprehensive interface for interacting with the Schwab API,
handling OAuth2 authentication, token management, and data access operations.

Features:
- Unified API client supporting both authentication and data operations
- OAuth2 token management with automatic refresh
- Database-backed credential and token storage
- Synchronous data access methods (quotes, user preferences)
- Support for both single and dual API configurations
- Retry logic with exponential backoff
- Email notifications for critical failures
- Secure credential handling

Classes:
- SchwabAPI: Unified client for authentication and data operations

Dependencies:
- httpx: For async HTTP requests
- requests: For synchronous HTTP requests
- Database connection via tools.db
- Email notifications via tools.emailer
"""

import time
import asyncio
import base64
import httpx
import os
import math
import requests
from urllib.parse import urlparse, parse_qs, unquote
from tools.emailer import send_email
from tools.db import DB
from tools.schwab_endpoints import SCHWAB_ENDPOINTS
from tools.decorators import retry_httpx
from tools.config import get_config
from datetime import datetime, date
from typing import List, Optional


class SchwabAPI:
    """
    Unified Schwab API client for handling OAuth2 authentication and API interactions.

    This class manages the complete lifecycle of Schwab API authentication and provides
    data access methods. It supports both single API mode and separate data/trade APIs.

    Features:
    - OAuth2 token acquisition and refresh with automatic expiration handling
    - Database-backed credential and token storage
    - Synchronous data access methods (quotes, user preferences)
    - Support for both single and dual API configurations
    - Retry logic with exponential backoff
    - Email notifications for critical failures

    Attributes:
        db (DB): Database connection instance
        name (str): Primary API configuration name identifier
        data_name (str): Data API configuration name (if separate)
        trade_name (str): Trade API configuration name (if separate)
        credentials (dict): Client credentials (ID, secret, redirect URI)
        tokens (dict): Current OAuth2 tokens and expiration times
        data_base_url (str): Base URL for data API endpoints
        trade_base_url (str): Base URL for trading API endpoints
    """
    def __init__(self, name, data_name=None, trade_name=None):
        """
        Initialize the Schwab API client.

        Args:
            name (str): The primary API configuration name to load from database.
                       This should match a record in the OPT.SCHWAB.API table.
            data_name (str, optional): Separate data API configuration name.
                                     If None, uses 'name' for data operations.
            trade_name (str, optional): Separate trade API configuration name.
                                      If None, uses 'name' for trade operations.

        Raises:
            ValueError: If no credentials are found for the given name(s).

        Examples:
            # Single API mode (backward compatible)
            api = SchwabAPI("MAIN_DATA")

            # Dual API mode
            api = SchwabAPI("MAIN_DATA", data_name="DATA_API", trade_name="TRADE_API")
        """
        self.db = DB()
        self.name = name
        self.data_name = data_name or name
        self.trade_name = trade_name or name

        # Load primary credentials and tokens
        self.credentials = self.load_credentials()
        self.tokens = {}
        self.load_tokens_from_db()

        # Load account hash for account-specific operations
        self.account_hash = self.load_account_hash_from_db()

        # Load and validate environment configuration
        self._load_config()

        # Initialize data/trade specific attributes for SchwabClient compatibility
        self._init_data_trade_config()

    def load_account_hash_from_db(self):
        """
        Load account hash from the SCHWAB.HASH table for account-specific API operations.

        Queries the database for the most recent account hash record associated with
        this API instance name. The account hash is required for Schwab API endpoints
        that operate on specific accounts (balance, orders, etc.).

        Returns:
            str: The account hash from the most recent record, or None if not found

        Database Schema Expected:
            SCHWAB.HASH table with columns:
            - Name: API instance name (matches self.name)
            - account_hash: 64-character hash value from Schwab
            - account_number: 8-digit account number
            - update_time: Timestamp of when the hash was stored

        Example:
            api = SchwabAPI("MAIN_TRADE")
            hash_value = api.load_account_hash_from_db()
            if hash_value:
                # Can now make account-specific API calls
                balance = await api.get_account_balance()

        Note:
            If no hash is found, account-specific operations will fail.
            The hash should be obtained and stored separately via the account
            numbers endpoint and hash extraction process.
        """
        query = """
            SELECT TOP 1 account_hash, account_number, update_time
            FROM SCHWAB.HASH
            WHERE Name = :name
            ORDER BY update_time DESC
        """

        try:
            result = self.db.execute_query(query, {"name": self.name})
            if result:
                # Return the account_hash from the most recent record
                account_hash = result[0].account_hash
                print(f"Loaded account hash for {self.name}: {account_hash[:8]}...")
                return account_hash
            else:
                print(f"Warning: No account hash found in SCHWAB.HASH table for API name '{self.name}'")
                print("Account-specific operations (balance, orders) will not work without an account hash.")
                return None
        except Exception as e:
            print(f"Error loading account hash for {self.name}: {str(e)}")
            return None

    def _load_config(self):
        """
        Load and validate configuration from centralized config system.

        Uses the centralized configuration manager to load API settings,
        combining configuration from config.yaml with endpoint definitions.

        Raises:
            ValueError: If configuration values are invalid.
        """
        try:
            # Get configuration from centralized config system
            config = get_config()
            api_config = config.get_api_config()
            tokens_config = config.get_service_config('tokens')

            # Load configuration with validation
            self.config = {
                'oauth_base_url': SCHWAB_ENDPOINTS.OAUTH_BASE_URL,
                'max_retries': api_config.get('max_retries', 30),
                'initial_retry_delay': api_config.get('initial_retry_delay', 1),
                'http_timeout': api_config.get('http_timeout', 10),
                'refresh_threshold': tokens_config.get('refresh_threshold', 60)
            }

            # Validate configuration values
            if self.config['max_retries'] <= 0:
                raise ValueError("max_retries must be greater than 0")
            if self.config['initial_retry_delay'] <= 0:
                raise ValueError("initial_retry_delay must be greater than 0")
            if self.config['http_timeout'] <= 0:
                raise ValueError("http_timeout must be greater than 0")
            if self.config['refresh_threshold'] < 0:
                raise ValueError("refresh_threshold must be non-negative")

        except ValueError as e:
            raise ValueError(f"Invalid configuration: {e}")

    def _init_data_trade_config(self):
        """
        Initialize data and trade specific configuration for API endpoints.

        Uses centralized endpoint configuration to ensure consistent URL management
        across OAuth, market data, and trading operations.
        """
        # Use centralized endpoint configuration
        self.data_base_url = SCHWAB_ENDPOINTS.MARKET_DATA_BASE_URL
        self.trade_base_url = SCHWAB_ENDPOINTS.TRADING_BASE_URL

        # Initialize token cache for quick access
        self._token_data_cache = None
        self._token_trade_cache = None

    def load_credentials(self):
        """
        Load API credentials from the database.

        Retrieves client_id, client_secret, and redirect_uri for the specified
        API configuration name from the OPT.SCHWAB.API table.

        Returns:
            dict: Dictionary containing client_id, client_secret, and redirect_uri

        Raises:
            ValueError: If no credentials are found for the configured name
        """
        query = "SELECT client_id, client_secret, redirect_uri FROM OPT.SCHWAB.API WHERE Name=:name"
        result = self.db.execute_query(query, {"name": self.name})
        if result:
            row = result[0]
            return {
                "client_id": row.client_id,
                "client_secret": row.client_secret,
                "redirect_uri": row.redirect_uri
            }
        else:
            raise ValueError(f"No credentials found for {self.name}")

    def build_headers(self):
        """
        Build HTTP headers for Schwab API authentication.

        Creates the Authorization header using Basic authentication with
        base64-encoded client credentials as required by OAuth2 spec.

        Returns:
            dict: HTTP headers including Authorization and Content-Type
        """
        cred = self.credentials
        # Encode client credentials for Basic authentication
        auth_string = f"{cred['client_id']}:{cred['client_secret']}"
        encoded_auth = base64.b64encode(auth_string.encode()).decode()

        return {
            'Authorization': f'Basic {encoded_auth}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

    def save_tokens_to_db(self):
        """
        Save current OAuth2 tokens to the database.

        Updates the database record with current access_token, refresh_token,
        and their respective expiration timestamps. Converts Unix timestamps
        to database-compatible datetime format.
        """
        data = self.tokens
        query = """
            UPDATE OPT.SCHWAB.API
            SET access_token=:access_token, refresh_token=:refresh_token,
                access_token_expires_at=:access_exp, refresh_token_expires_at=:refresh_exp
            WHERE Name=:name
        """
        # Convert Unix timestamps to database datetime format
        self.db.execute_non_query(query, {
            "access_token": data.get('access_token'),
            "refresh_token": data.get('refresh_token'),
            "access_exp": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(data.get('access_token_expires_at', 0))),
            "refresh_exp": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(data.get('refresh_token_expires_at', 0))),
            "name": self.name
        })

    def load_tokens_from_db(self):
        """
        Load existing OAuth2 tokens from the database.

        Retrieves stored tokens and converts database datetime format back to
        Unix timestamps for internal use. If no tokens exist, initializes
        empty token dictionary.
        """
        query = "SELECT refresh_token, access_token, access_token_expires_at, refresh_token_expires_at FROM OPT.SCHWAB.API WHERE Name=:name"
        result = self.db.execute_query(query, {"name": self.name})
        if result:
            row = result[0]
            # Convert database datetime to Unix timestamp for internal use
            self.tokens = {
                'refresh_token': row.refresh_token,
                'access_token': row.access_token,
                'access_token_expires_at': (row.access_token_expires_at - datetime(1970, 1, 1)).total_seconds() if row.access_token_expires_at else 0,
                'refresh_token_expires_at': (row.refresh_token_expires_at - datetime(1970, 1, 1)).total_seconds() if row.refresh_token_expires_at else 0
            }
        else:
            # Initialize empty tokens if none exist in database
            self.tokens = {}

    def get_token_uri(self):
        """
        Generate the OAuth2 authorization URL for Schwab API.

        Creates the URL that users need to visit to authorize the application
        and grant access to their Schwab account.

        Returns:
            str: Complete authorization URL with client_id and redirect_uri parameters
        """
        credential = self.credentials
        # Use proper OAuth endpoint
        return f"{SCHWAB_ENDPOINTS.oauth_authorize}?client_id={credential['client_id']}&redirect_uri={credential['redirect_uri']}"

    @retry_httpx(max_retries=3, initial_delay=1)
    async def parse_uri_to_get_tokens(self, redirected_url):
        """
        Extract authorization code from redirect URL and exchange for tokens.

        Parses the redirect URL to extract the authorization code, then exchanges
        it for access and refresh tokens via the Schwab token endpoint.

        Args:
            redirected_url (str): The URL the user was redirected to after authorization

        Note:
            Automatically saves tokens to database and sets expiration times.
            Refresh token expires after 7 days (hardcoded value).
            Retry logic is handled by the @retry_httpx decorator.
        """
        credential = self.credentials

        # Parse the redirect URL to extract authorization code
        parsed_url = urlparse(redirected_url)
        query_params = parse_qs(parsed_url.query)
        authorization_code = query_params.get('code')[0]

        # Prepare token exchange request
        headers = self.build_headers()
        data = {
            'grant_type': 'authorization_code',
            'code': unquote(authorization_code),
            'redirect_uri': credential['redirect_uri']
        }

        # Exchange authorization code for tokens
        async with httpx.AsyncClient(timeout=self.config['http_timeout']) as client:
            response = await client.post(SCHWAB_ENDPOINTS.oauth_token, headers=headers, data=data)
            response.raise_for_status()  # Raise exception for HTTP errors

        tokens_response = response.json()
        self.tokens.update(tokens_response)

        # Set token expiration times
        self.tokens['access_token_expires_at'] = time.time() + tokens_response['expires_in']
        # Refresh token expiry is set by Schwab (7 days) - keep hardcoded
        self.tokens['refresh_token_expires_at'] = time.time() + 7 * 24 * 60 * 60  # 7 days

        self.save_tokens_to_db()

    @retry_httpx(max_retries=30, initial_delay=1)
    async def get_new_access_token(self):
        """
        Retrieve a new access token using the refresh token with retry logic.

        Uses the retry_httpx decorator to handle temporary network issues or API rate limiting.
        Sends email notifications on failures and automatically stores tokens on success.

        Features:
        - Retry logic handled by decorator with exponential backoff
        - Email notifications for failures
        - Automatic token storage on success

        Note:
            Retry configuration is handled by the @retry_httpx decorator.
        """
        headers = self.build_headers()
        credentials = self.load_credentials()

        # Prepare refresh token request
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.tokens.get("refresh_token", credentials.get("refresh_token", ""))
        }

        try:
            async with httpx.AsyncClient(timeout=self.config['http_timeout']) as client:
                response = await client.post(SCHWAB_ENDPOINTS.oauth_token, headers=headers, data=data)
                response.raise_for_status()  # Raise exception for HTTP errors

                # Success: update tokens and save to database
                tokens_response = response.json()
                self.tokens.update(tokens_response)
                self.tokens["access_token_expires_at"] = time.time() + tokens_response["expires_in"]
                self.save_tokens_to_db()
                return

        except httpx.HTTPStatusError as e:
            # HTTP error: log and send email notification
            error_msg = f"Token refresh failed. Status: {e.response.status_code}, Response: {e.response.text}"
            print(f"⚠️ {error_msg}")
            send_email("TOKENS_MANAGER | GetToken Failed:", error_msg)
            raise  # Re-raise to trigger decorator retry

        except Exception as e:
            # Unexpected error: log and send email notification
            error_msg = f"Unexpected error during token refresh: {e}"
            print(f"⚠️ {error_msg}")
            send_email("TOKENS_MANAGER | Unexpected error:", str(e))
            raise  # Re-raise to trigger decorator retry

    async def token_handler(self):
        """
        Handle token expiration and refresh logic.

        Checks token expiration status and automatically refreshes tokens when needed.
        Uses a configurable threshold to refresh tokens before they expire.

        Logic:
        1. If refresh token is expired: Initiate full re-authorization flow
        2. If access token is near expiry: Refresh using refresh token

        Note:
            Configuration values are loaded from environment variables during initialization.
        """
        # Use pre-loaded and validated configuration
        refresh_threshold = self.config['refresh_threshold']
        current_time = time.time()

        # Calculate time until access token expires (minus threshold)
        access_token_expires_in = self.tokens.get('access_token_expires_at', 0) - current_time - refresh_threshold

        if current_time >= self.tokens.get('refresh_token_expires_at', 0):
            # Refresh token has expired - need full re-authorization
            print('Refresh Token Expired')
            await self.get_new_refresh_token()
        elif access_token_expires_in <= 0:
            # Access token is near expiry - refresh using refresh token
            # Uncomment for debugging: print('Access Token Expired')
            await self.get_new_access_token()

    async def get_new_refresh_token(self):
        """
        Initiate the full OAuth2 authorization flow to get new refresh token.

        This method is called when the refresh token has expired and requires
        user interaction to re-authorize the application. Displays the authorization
        URL and prompts for the redirect URL containing the authorization code.

        Note:
            This is an interactive method that requires user input via console.
            In production, consider implementing a web-based authorization flow.
        """
        # Generate authorization URL
        token_uri = self.get_token_uri()
        print(f"Please visit this URL and authorize the app: {token_uri}")

        # Get redirect URL from user (contains authorization code)
        redirected_url = input("Enter the redirected URL: ")

        # Exchange authorization code for new tokens
        await self.parse_uri_to_get_tokens(redirected_url)

    # ═══════════════════════════════════════════════════════════════════════════════
    # Token Access Properties (for SchwabClient compatibility)
    # ═══════════════════════════════════════════════════════════════════════════════

    @property
    def token_data(self) -> str:
        """
        Get the current data API access token.

        Returns the access token for data operations. If using separate data/trade
        APIs, returns the data API token. Otherwise returns the primary token.

        Returns:
            str: Current access token for data operations
        """
        if self.data_name != self.name:
            # Using separate data API - get token from database
            return self.db.get_token(self.data_name)
        else:
            # Using primary API for data operations
            return self.tokens.get('access_token', '')

    @property
    def token_trade(self) -> str:
        """
        Get the current trade API access token.

        Returns the access token for trading operations. If using separate data/trade
        APIs, returns the trade API token. Otherwise returns the primary token.

        Returns:
            str: Current access token for trading operations
        """
        if self.trade_name != self.name:
            # Using separate trade API - get token from database
            return self.db.get_token(self.trade_name)
        else:
            # Using primary API for trade operations
            return self.tokens.get('access_token', '')

    # ═══════════════════════════════════════════════════════════════════════════════
    # Synchronous Data Access Methods (from SchwabClient)
    # ═══════════════════════════════════════════════════════════════════════════════

    def get_user_preferences(self) -> dict:
        """
        Fetch user preferences including streamer information.

        Retrieves user preferences from Schwab API, which includes
        streaming session tokens and WebSocket URLs needed for
        real-time data streaming.

        Returns:
            dict: User preferences including streamerInfo with session tokens
                 and socket URLs for WebSocket connections

        Raises:
            requests.HTTPError: If the API request fails

        Example:
            api = SchwabAPI("MAIN_TRADE")
            prefs = api.get_user_preferences()
            streamer_info = prefs["streamerInfo"][0]
            socket_url = streamer_info["streamerSocketUrl"]
        """
        resp = requests.get(
            SCHWAB_ENDPOINTS.user_preferences,
            headers={"Authorization": f"Bearer {self.token_trade}"}
        )
        resp.raise_for_status()
        return resp.json()

    def get_option_quotes(self, symbols: list[str]) -> dict:
        """
        Fetch option quotes for the specified symbols.

        Retrieves current market data for the provided option symbols
        from the Schwab data API.

        Args:
            symbols (list[str]): List of option symbols to fetch quotes for
                               (e.g., ['SPXW  241218C05000000', 'SPXW  241218P05000000'])

        Returns:
            dict: Option quote data from Schwab API

        Raises:
            requests.HTTPError: If the API request fails

        Example:
            api = SchwabAPI("MAIN_DATA")
            quotes = api.get_option_quotes(['SPXW  241218C05000000'])
        """
        resp = requests.get(
            SCHWAB_ENDPOINTS.option_quotes,
            headers={"Authorization": f"Bearer {self.token_data}"},
            params={"symbols": ",".join(symbols)}
        )
        resp.raise_for_status()
        return resp.json()

    def get_underlying_quote(self, symbol: str) -> float:
        """
        Fetch the latest underlying quote for a symbol.

        Retrieves the current last price for an underlying security
        (e.g., '$SPX') from the Schwab data API.

        Args:
            symbol (str): The underlying symbol to fetch (e.g., '$SPX')

        Returns:
            float: The last price of the underlying security

        Raises:
            requests.HTTPError: If the API request fails
            ValueError: If no quote data is returned for the symbol

        Example:
            api = SchwabAPI("MAIN_DATA")
            spx_price = api.get_underlying_quote('$SPX')
            print(f"SPX last price: {spx_price}")
        """
        resp = requests.get(
            SCHWAB_ENDPOINTS.quotes,
            headers={"Authorization": f"Bearer {self.token_data}"},
            params={"symbols": symbol}
        )
        resp.raise_for_status()
        data = resp.json()  # e.g. {"$SPX": { ..., "quote": { "lastPrice": 5921.54, ... } }}

        # Pull out the entry for our symbol
        entry = data.get(symbol)
        if not entry:
            # If it comes under a "quotes" or "data" list, fall back
            for key in ("quotes", "data"):
                if key in data and isinstance(data[key], list) and data[key]:
                    entry = data[key][0]
                    break

        if not entry or "quote" not in entry or "lastPrice" not in entry["quote"]:
            raise ValueError(f"No quote data returned for symbol {symbol!r}: {data!r}")

        return float(entry["quote"]["lastPrice"])

    async def get_accounts(self) -> list:
        """
        Fetch account numbers and their encrypted hash values from Schwab API.

        Retrieves the list of account numbers and their corresponding hash values
        for the authenticated user. This is typically used to get account hashes
        that are required for account-specific API operations.

        Returns:
            list: List of account objects, each containing:
                - accountNumber: The account number (8 digits)
                - hashValue: The encrypted account hash (64 characters)

        Raises:
            requests.HTTPError: If the API request fails
            Exception: If token refresh is needed and fails

        Example:
            api = SchwabAPI("MAIN_TRADE")
            accounts = await api.get_accounts()
            for account in accounts:
                print(f"Account: {account['accountNumber']}, Hash: {account['hashValue']}")

        Note:
            This method uses the trade token for authentication and may trigger
            automatic token refresh if the current token is expired.
        """
        import requests

        try:
            # Make request to account numbers endpoint
            resp = requests.get(
                SCHWAB_ENDPOINTS.account_numbers,
                headers={"Authorization": f"Bearer {self.token_trade}"}
            )
            resp.raise_for_status()
            return resp.json()

        except requests.HTTPError as e:
            if e.response.status_code == 401:
                # Token expired - attempt refresh and retry once
                try:
                    await self.get_new_access_token()
                    # Retry the request with refreshed token
                    resp = requests.get(
                        SCHWAB_ENDPOINTS.account_numbers,
                        headers={"Authorization": f"Bearer {self.token_trade}"}
                    )
                    resp.raise_for_status()
                    return resp.json()

                except Exception as refresh_error:
                    raise Exception(f"Failed to refresh token and retry accounts request: {str(refresh_error)}") from refresh_error
            else:
                # Re-raise other HTTP errors
                raise
        except Exception as e:
            # Re-raise with more context for debugging
            if not isinstance(e, requests.HTTPError):
                raise Exception(f"Error fetching accounts: {str(e)}") from e
            raise

    def get_account_hash(self, accounts_response: list, account_number: str) -> str:
        """
        Extract account hash from the accounts API response for a specific account number.

        Searches through the accounts response to find the hash value for the
        specified account number. This hash is required for account-specific
        API operations like getting balances or placing orders.

        Args:
            accounts_response (list): Response from get_accounts() API call
            account_number (str): The account number to find the hash for

        Returns:
            str: The account hash (hashValue) for the specified account number,
                 or None if the account number is not found

        Example:
            api = SchwabAPI("MAIN_TRADE")
            accounts = await api.get_accounts()
            hash_value = api.get_account_hash(accounts, "12345678")
            if hash_value:
                print(f"Hash for account 12345678: {hash_value}")

        Note:
            Both account_number parameter and accountNumber in response are
            converted to strings for comparison to handle type mismatches.
        """
        for account in accounts_response:
            if str(account.get("accountNumber")) == str(account_number):
                return account.get("hashValue")

        print(f"Account number {account_number} not found in API response.")
        return None

    async def get_account_balance(self) -> dict:
        """
        Fetch account balance and position data from Schwab API using account hash.

        Retrieves comprehensive account information including balances, positions,
        and account metadata from the Schwab trading API. This method uses the
        account-specific endpoint with the cached account hash.

        Returns:
            dict: Account data from Schwab API containing:
                - securitiesAccount: Account details including:
                  - accountNumber: Account identifier
                  - roundTrips: Number of round trips for day trading
                  - isDayTrader: Day trader status
                  - isClosingOnlyRestricted: Closing only restriction status
                  - currentBalances: Financial balance information including:
                    - dayTradingBuyingPower: Available day trading buying power
                    - cashBalance: Cash balance in the account
                    - liquidationValue: Total liquidation value
                - positions: List of current positions (if any)

        Raises:
            ValueError: If no account hash is available or no account data is returned
            requests.HTTPError: If the API request fails
            Exception: If token refresh is needed and fails

        Example:
            api = SchwabAPI("MAIN_TRADE")
            account_data = await api.get_account_balance()
            account_info = account_data.get("securitiesAccount", {})
            balance_info = account_info.get("currentBalances", {})
            buying_power = balance_info.get("dayTradingBuyingPower")

        Note:
            This method requires an account hash to be loaded from the SCHWAB.HASH table.
            If no hash is available, the method will attempt to reload it once.
        """
        import requests

        # Ensure we have an account hash
        if not self.account_hash:
            print("No account hash available, attempting to reload from database...")
            self.account_hash = self.load_account_hash_from_db()

        if not self.account_hash:
            raise ValueError("Cannot fetch account balance: No account hash available. "
                           "Ensure the SCHWAB.HASH table contains a record for this API name.")

        try:
            # Make request to specific account endpoint using account hash
            account_url = SCHWAB_ENDPOINTS.account_by_number(self.account_hash)
            resp = requests.get(
                account_url,
                headers={"Authorization": f"Bearer {self.token_trade}"}
            )
            resp.raise_for_status()

            # Parse response
            account_data = resp.json()

            if not account_data:
                raise ValueError("No account data returned from Schwab API")

            return account_data

        except requests.HTTPError as e:
            if e.response.status_code == 401:
                # Token expired - attempt refresh and retry once
                try:
                    await self.get_new_access_token()
                    # Retry the request with refreshed token
                    account_url = SCHWAB_ENDPOINTS.account_by_number(self.account_hash)
                    resp = requests.get(
                        account_url,
                        headers={"Authorization": f"Bearer {self.token_trade}"}
                    )
                    resp.raise_for_status()
                    account_data = resp.json()

                    if not account_data:
                        raise ValueError("No account data returned from Schwab API after token refresh")

                    return account_data

                except Exception as refresh_error:
                    raise Exception(f"Failed to refresh token and retry account balance request: {str(refresh_error)}") from refresh_error
            else:
                # Re-raise other HTTP errors
                raise
        except Exception as e:
            # Re-raise with more context for debugging
            if not isinstance(e, (requests.HTTPError, ValueError)):
                raise Exception(f"Error fetching account balance: {str(e)}") from e
            raise

    @retry_httpx(max_retries=3, initial_delay=1)
    async def get_markets(self, markets: str, date) -> dict:
        """
        Fetch market hours data from Schwab API.

        Retrieves market hours information for specified markets and date from the
        Schwab market data API. This is used to determine market open/close times
        and trading session schedules.

        Args:
            markets (str): Market type to query (e.g., 'equity', 'option', 'bond', 'forex')
            date (date): Date to get market hours for (datetime.date object)

        Returns:
            dict: Market hours data from Schwab API containing market session information

        Raises:
            httpx.HTTPError: If the API request fails
            Exception: If token refresh is needed and fails

        Example:
            api = SchwabAPI("MAIN_DATA")
            from datetime import date
            market_data = await api.get_markets('equity', date.today())
            equity_info = market_data.get('equity', {})
            is_open = equity_info.get('isOpen', False)

        Note:
            This method uses the data token for authentication and automatic retry
            logic is handled by the @retry_httpx decorator.
        """
        # Ensure we have valid tokens
        await self.token_handler()

        # Format date as string for API parameter
        date_str = date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date)

        try:
            # Make request to market hours endpoint
            async with httpx.AsyncClient(timeout=self.config['http_timeout']) as client:
                resp = await client.get(
                    SCHWAB_ENDPOINTS.market_hours,
                    headers={"Authorization": f"Bearer {self.token_data}"},
                    params={'markets': markets, 'date': date_str}
                )
                resp.raise_for_status()
                return resp.json()

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                # Token expired - attempt refresh and retry
                await self.get_new_access_token()
                # Re-raise to trigger decorator retry with refreshed token
                raise
            else:
                # Re-raise other HTTP errors
                raise
        except Exception as e:
            # Re-raise with more context for debugging
            if not isinstance(e, httpx.HTTPStatusError):
                raise Exception(f"Error fetching market hours: {str(e)}") from e
            raise

    @retry_httpx(max_retries=3, initial_delay=1)
    async def get_history(self, symbol: str, period: int, periodType: str, frequency: int, frequencyType: str) -> dict:
        """
        Fetch OHLC price history from Schwab API.

        Retrieves historical price data (OHLC candles) for a specified symbol
        from the Schwab market data API with configurable time periods and frequencies.

        Args:
            symbol (str): The symbol to fetch history for (e.g., '$SPX', '$VIX')
            period (int): Number of periods to retrieve
            periodType (str): Type of period ('day', 'month', 'year', 'ytd')
            frequency (int): Frequency of data points
            frequencyType (str): Type of frequency ('minute', 'daily', 'weekly', 'monthly')

        Returns:
            dict: Historical price data from Schwab API containing 'candles' array

        Raises:
            httpx.HTTPError: If the API request fails
            Exception: If token refresh is needed and fails

        Example:
            api = SchwabAPI("MAIN_DATA")
            # Get 10 days of minute data
            history = await api.get_history('$SPX', 10, 'day', 1, 'minute')
            candles = history.get('candles', [])

        Note:
            This method uses the data token for authentication and automatic retry
            logic is handled by the @retry_httpx decorator.
        """
        # Ensure we have valid tokens
        await self.token_handler()

        try:
            # Make request to price history endpoint
            async with httpx.AsyncClient(timeout=self.config['http_timeout']) as client:
                resp = await client.get(
                    SCHWAB_ENDPOINTS.price_history,
                    headers={"Authorization": f"Bearer {self.token_data}"},
                    params={
                        "symbol": symbol,
                        "period": period,
                        "periodType": periodType,
                        "frequency": frequency,
                        "frequencyType": frequencyType
                    }
                )
                resp.raise_for_status()
                return resp.json()

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                # Token expired - attempt refresh and retry
                await self.get_new_access_token()
                # Re-raise to trigger decorator retry with refreshed token
                raise
            else:
                # Re-raise other HTTP errors
                raise
        except Exception as e:
            # Re-raise with more context for debugging
            if not isinstance(e, httpx.HTTPStatusError):
                raise Exception(f"Error fetching price history for {symbol}: {str(e)}") from e
            raise

    async def get_historic_quote_to_sql_minute(self, symbol: str):
        """
        Fetch minute-level historical data and store in SQL database.

        Retrieves 10 days of minute-level OHLC data for the specified symbol
        and stores it in the database using the df_to_sql method.

        Args:
            symbol (str): The symbol to fetch data for (e.g., '$SPX', '$VIX')

        Note:
            This method processes the data by:
            - Converting timestamps to Pacific Time
            - Adding symbol and frequency metadata
            - Storing in the PYTHON.MINUTE table
        """
        print(f"🔑 Getting historic minute quotes: {symbol}")

        try:
            # Fetch 10 days of minute data
            result = await self.get_history(symbol, period=10, periodType='day', frequency=1, frequencyType='minute')

            if not result or 'candles' not in result:
                print(f"No minute data available for {symbol}")
                return

            # Convert to DataFrame and process
            import pandas as pd
            df = pd.DataFrame.from_records(result['candles'])

            # Convert datetime from milliseconds to Pacific Time
            df['datetime'] = pd.to_datetime(df['datetime'], utc=True, unit='ms').dt.tz_convert('America/Los_Angeles').dt.tz_localize(None)
            df['Symbol'] = symbol.lstrip("$^")  # Remove $ and ^ prefixes
            df['freq'] = 'MINUTE'

            # Store in database
            self.db.df_to_sql(df, 'MINUTE', if_exists='append', schema_name='PYTHON')
            print(f"✅ Stored {len(df)} minute records for {symbol}")

        except Exception as e:
            print(f"❌ Error fetching minute data for {symbol}: {str(e)}")
            raise

    async def get_historic_quote_to_sql_day(self, symbol: str):
        """
        Fetch daily historical data and store in SQL database.

        Retrieves 2 months of daily OHLC data for the specified symbol
        and stores it in the database using the df_to_sql method.

        Args:
            symbol (str): The symbol to fetch data for (e.g., '$SPX', '$VIX')

        Note:
            This method processes the data by:
            - Converting timestamps to date format
            - Adding symbol and frequency metadata
            - Storing in the PYTHON.DAY table
        """
        print(f"🔑 Getting historic day quotes: {symbol}")

        try:
            # Fetch 2 months of daily data
            result = await self.get_history(symbol, period=2, periodType='month', frequency=1, frequencyType='daily')

            if not result or 'candles' not in result:
                print(f"No daily data available for {symbol}")
                return

            # Convert to DataFrame and process
            import pandas as pd
            df = pd.DataFrame.from_records(result['candles'])

            # Convert datetime from milliseconds and extract date
            df['datetime'] = pd.to_datetime(df['datetime'], utc=True, unit='ms').dt.tz_localize(None)
            df['date'] = df['datetime'].dt.date
            df['Symbol'] = symbol.lstrip("$^")  # Remove $ and ^ prefixes
            df['freq'] = 'DAY'

            # Store in database
            self.db.df_to_sql(df, 'DAY', if_exists='append', schema_name='PYTHON')
            print(f"✅ Stored {len(df)} daily records for {symbol}")

        except Exception as e:
            print(f"❌ Error fetching daily data for {symbol}: {str(e)}")
            raise

    @retry_httpx(max_retries=3, initial_delay=1)
    async def get_orders(self, account_hash: str, start_date: str, end_date: str) -> list:
        """
        Fetch orders from Schwab API for a specific account and date range.

        Retrieves order information for the specified account hash within the
        given date range from the Schwab trading API.

        Args:
            account_hash (str): The account hash for the specific account
            start_date (str): Start date in ISO format (e.g., '2024-12-18T00:00:00.000Z')
            end_date (str): End date in ISO format (e.g., '2024-12-19T00:00:00.000Z')

        Returns:
            list: List of order objects from Schwab API

        Raises:
            httpx.HTTPError: If the API request fails
            Exception: If token refresh is needed and fails

        Example:
            api = SchwabAPI("MAIN_TRADE")
            from datetime import datetime, timezone, timedelta
            now = datetime.now(timezone.utc)
            end_date = now.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + "Z"
            start_date = (now - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + "Z"
            orders = await api.get_orders(account_hash, start_date, end_date)

        Note:
            This method uses the trade token for authentication and automatic retry
            logic is handled by the @retry_httpx decorator.
        """
        # Ensure we have valid tokens
        await self.token_handler()

        try:
            # Make request to account orders endpoint
            orders_url = SCHWAB_ENDPOINTS.account_orders(account_hash)
            async with httpx.AsyncClient(timeout=self.config['http_timeout']) as client:
                resp = await client.get(
                    orders_url,
                    headers={"Authorization": f"Bearer {self.token_trade}"},
                    params={"fromEnteredTime": start_date, "toEnteredTime": end_date}
                )
                resp.raise_for_status()
                return resp.json()

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                # Token expired - attempt refresh and retry
                await self.get_new_access_token()
                # Re-raise to trigger decorator retry with refreshed token
                raise
            else:
                # Re-raise other HTTP errors
                raise
        except Exception as e:
            # Re-raise with more context for debugging
            if not isinstance(e, httpx.HTTPStatusError):
                raise Exception(f"Error fetching orders for account {account_hash}: {str(e)}") from e
            raise

    @retry_httpx(max_retries=3, initial_delay=1)
    async def get_chains(self, symbol: str, fromDTE: int, toDTE: int, strikeCount: int) -> dict:
        """
        Fetch option chains from Schwab API.

        Retrieves option chain data for the specified symbol with configurable
        expiration date range and strike count from the Schwab market data API.

        Args:
            symbol (str): The underlying symbol (e.g., '$SPX', 'AAPL')
            fromDTE (int): Starting days to expiration (0 = today)
            toDTE (int): Ending days to expiration
            strikeCount (int): Number of strikes to retrieve around current price

        Returns:
            dict: Option chain data from Schwab API containing:
                - callExpDateMap: Call options organized by expiration date and strike
                - putExpDateMap: Put options organized by expiration date and strike
                - underlying: Information about the underlying security

        Raises:
            httpx.HTTPError: If the API request fails
            Exception: If token refresh is needed and fails

        Example:
            api = SchwabAPI("MAIN_DATA")
            # Get current day options with 200 strikes
            chains = await api.get_chains('$SPX', 0, 0, 200)
            calls = chains.get('callExpDateMap', {})
            puts = chains.get('putExpDateMap', {})

        Note:
            This method uses the data token for authentication and automatic retry
            logic is handled by the @retry_httpx decorator.
            The fromDate and toDate parameters are calculated using get_dte_date().
        """
        from tools.utils import get_dte_date

        # Ensure we have valid tokens
        await self.token_handler()

        try:
            # Make request to option chains endpoint
            async with httpx.AsyncClient(timeout=self.config['http_timeout']) as client:
                resp = await client.get(
                    SCHWAB_ENDPOINTS.option_chains,
                    headers={"Authorization": f"Bearer {self.token_data}"},
                    params={
                        'symbol': symbol,
                        'fromDate': get_dte_date(fromDTE).strftime('%Y-%m-%d'),
                        'toDate': get_dte_date(toDTE).strftime('%Y-%m-%d'),
                        'strikeCount': strikeCount
                    }
                )
                resp.raise_for_status()
                return resp.json()

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                # Token expired - attempt refresh and retry
                await self.get_new_access_token()
                # Re-raise to trigger decorator retry with refreshed token
                raise
            else:
                # Re-raise other HTTP errors
                raise
        except Exception as e:
            # Re-raise with more context for debugging
            if not isinstance(e, httpx.HTTPStatusError):
                raise Exception(f"Error fetching option chains for {symbol}: {str(e)}") from e
            raise


def generate_spxw_symbols(
    spx_price: float,
    range_width: float = 100,
    strike_step: float = 5,
    expiry_date: Optional[date] = None
) -> List[str]:
    """
    Generate SPXW 0DTE symbols for a given expiry_date (defaults to today).

    Creates option symbols for strikes in [spx_price - range_width, spx_price + range_width],
    stepping by strike_step (5-point increments).

    Args:
        spx_price (float): Current SPX price to center the strike range around
        range_width (float): Points above and below SPX price to include (default: 100)
        strike_step (float): Strike price increment in points (default: 5)
        expiry_date (Optional[date]): Option expiration date (default: today)

    Returns:
        List[str]: List of SPXW option symbols in Schwab format

    Format:
        SPXW<two spaces><YYMMDD><C|P><8-digit zero-padded strike×1000>

    Example:
        generate_spxw_symbols(5000.0, 50, 5) might return:
        ['SPXW  241218C04950000', 'SPXW  241218P04950000', ...]
    """
    if expiry_date is None:
        expiry_date = date.today()
    exp_code = expiry_date.strftime("%y%m%d")

    # Snap lower/upper bounds to multiples of strike_step
    low = math.floor((spx_price - range_width) / strike_step) * strike_step
    high = math.ceil((spx_price + range_width) / strike_step) * strike_step

    symbols: List[str] = []
    steps = int((high - low) / strike_step) + 1
    for i in range(steps):
        strike = low + i * strike_step
        code = str(int(strike * 1000)).zfill(8)
        for cp in ("C", "P"):
            symbols.append(f"SPXW  {exp_code}{cp}{code}")
    return symbols
