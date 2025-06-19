"""
Schwab API Endpoints Configuration

This module defines the official Schwab API endpoint structure and provides
a centralized configuration for all API calls.

Schwab API Structure:
- OAuth: https://api.schwabapi.com/v1 (for token operations)
- Market Data: https://api.schwabapi.com/marketdata/v1 (for quotes, options, etc.)
- Trading: https://api.schwabapi.com/trader/v1 (for accounts, orders, preferences, etc.)
"""

from typing import Optional
from dataclasses import dataclass


@dataclass
class SchwabEndpoints:
    """
    Centralized configuration for Schwab API endpoints.

    This class provides a single source of truth for all Schwab API URLs
    and endpoint mappings, eliminating confusion between OAuth and API operations.

    Base URLs are static and controlled by Schwab - no environment overrides needed.
    """

    # Base URLs - static and controlled by Schwab
    OAUTH_BASE_URL: str = "https://api.schwabapi.com/v1"
    MARKET_DATA_BASE_URL: str = "https://api.schwabapi.com/marketdata/v1" 
    TRADING_BASE_URL: str = "https://api.schwabapi.com/trader/v1"
    

    
    # OAuth Endpoints (always use OAUTH_BASE_URL)
    @property
    def oauth_authorize(self) -> str:
        """OAuth authorization endpoint."""
        return f"{self.OAUTH_BASE_URL}/oauth/authorize"
    
    @property 
    def oauth_token(self) -> str:
        """OAuth token endpoint."""
        return f"{self.OAUTH_BASE_URL}/oauth/token"
    
    # Market Data Endpoints (use MARKET_DATA_BASE_URL)
    @property
    def quotes(self) -> str:
        """Get quotes by list of symbols."""
        return f"{self.MARKET_DATA_BASE_URL}/quotes"

    def quote_by_symbol(self, symbol: str) -> str:
        """Get quote by single symbol."""
        return f"{self.MARKET_DATA_BASE_URL}/{symbol}/quotes"

    @property
    def option_chains(self) -> str:
        """Get option chain for an optionable symbol."""
        return f"{self.MARKET_DATA_BASE_URL}/chains"

    @property
    def option_expiration_chain(self) -> str:
        """Get option expiration chain for an optionable symbol."""
        return f"{self.MARKET_DATA_BASE_URL}/expirationchain"

    @property
    def price_history(self) -> str:
        """Get price history for a single symbol and date ranges."""
        return f"{self.MARKET_DATA_BASE_URL}/pricehistory"

    def movers(self, symbol_id: str) -> str:
        """Get movers for a specific index."""
        return f"{self.MARKET_DATA_BASE_URL}/movers/{symbol_id}"

    @property
    def market_hours(self) -> str:
        """Get market hours for different markets."""
        return f"{self.MARKET_DATA_BASE_URL}/markets"

    def market_hours_by_id(self, market_id: str) -> str:
        """Get market hours for a single market."""
        return f"{self.MARKET_DATA_BASE_URL}/markets/{market_id}"

    @property
    def instruments(self) -> str:
        """Get instruments by symbols and projections."""
        return f"{self.MARKET_DATA_BASE_URL}/instruments"

    def instrument_by_cusip(self, cusip_id: str) -> str:
        """Get instrument by specific cusip."""
        return f"{self.MARKET_DATA_BASE_URL}/instruments/{cusip_id}"
    
    # Trading Endpoints (use TRADING_BASE_URL)
    @property
    def account_numbers(self) -> str:
        """Get list of account numbers and their encrypted values."""
        return f"{self.TRADING_BASE_URL}/accounts/accountNumbers"

    @property
    def accounts(self) -> str:
        """Get linked account(s) balances and positions for the logged in user."""
        return f"{self.TRADING_BASE_URL}/accounts"

    def account_by_number(self, account_number: str) -> str:
        """Get a specific account balance and positions for the logged in user."""
        return f"{self.TRADING_BASE_URL}/accounts/{account_number}"

    def account_orders(self, account_number: str) -> str:
        """Get all orders for a specific account."""
        return f"{self.TRADING_BASE_URL}/accounts/{account_number}/orders"

    def place_order(self, account_number: str) -> str:
        """Place order for a specific account."""
        return f"{self.TRADING_BASE_URL}/accounts/{account_number}/orders"

    def get_order(self, account_number: str, order_id: str) -> str:
        """Get a specific order by its ID, for a specific account."""
        return f"{self.TRADING_BASE_URL}/accounts/{account_number}/orders/{order_id}"

    def cancel_order(self, account_number: str, order_id: str) -> str:
        """Cancel an order for a specific account."""
        return f"{self.TRADING_BASE_URL}/accounts/{account_number}/orders/{order_id}"

    def replace_order(self, account_number: str, order_id: str) -> str:
        """Replace order for a specific account."""
        return f"{self.TRADING_BASE_URL}/accounts/{account_number}/orders/{order_id}"

    @property
    def all_orders(self) -> str:
        """Get all orders for all accounts."""
        return f"{self.TRADING_BASE_URL}/orders"

    def preview_order(self, account_number: str) -> str:
        """Preview order for a specific account."""
        return f"{self.TRADING_BASE_URL}/accounts/{account_number}/previewOrder"

    def account_transactions(self, account_number: str) -> str:
        """Get all transactions information for a specific account."""
        return f"{self.TRADING_BASE_URL}/accounts/{account_number}/transactions"

    def account_transaction(self, account_number: str, transaction_id: str) -> str:
        """Get specific transaction information for a specific account."""
        return f"{self.TRADING_BASE_URL}/accounts/{account_number}/transactions/{transaction_id}"

    @property
    def user_preferences(self) -> str:
        """Get user preference information for the logged in user."""
        return f"{self.TRADING_BASE_URL}/userPreference"

# Global instance for easy access
SCHWAB_ENDPOINTS = SchwabEndpoints()


def get_endpoint_for_operation(operation: str) -> Optional[str]:
    """
    Get the appropriate endpoint URL for a given operation.
    
    Args:
        operation: The operation name (e.g., 'quotes', 'user_preferences', 'oauth_token')
        
    Returns:
        The full endpoint URL or None if operation not found
        
    Example:
        url = get_endpoint_for_operation('quotes')
        # Returns: "https://api.schwabapi.com/marketdata/v1/quotes"
    """
    endpoints = SCHWAB_ENDPOINTS
    
    # Map operation names to endpoint properties
    operation_map = {
        # OAuth operations
        'oauth_authorize': endpoints.oauth_authorize,
        'oauth_token': endpoints.oauth_token,

        # Market data operations
        'quotes': endpoints.quotes,
        'option_chains': endpoints.option_chains,
        'option_expiration_chain': endpoints.option_expiration_chain,
        'price_history': endpoints.price_history,
        'market_hours': endpoints.market_hours,
        'instruments': endpoints.instruments,

        # Trading operations
        'account_numbers': endpoints.account_numbers,
        'accounts': endpoints.accounts,
        'all_orders': endpoints.all_orders,
        'user_preferences': endpoints.user_preferences,
    }
    
    return operation_map.get(operation)


def get_base_url_for_service(service: str) -> str:
    """
    Get the base URL for a service type.
    
    Args:
        service: Service type ('oauth', 'market_data', 'trading')
        
    Returns:
        The base URL for the service
        
    Raises:
        ValueError: If service type is not recognized
    """
    endpoints = SCHWAB_ENDPOINTS
    
    service_map = {
        'oauth': endpoints.OAUTH_BASE_URL,
        'market_data': endpoints.MARKET_DATA_BASE_URL, 
        'trading': endpoints.TRADING_BASE_URL,
    }
    
    if service not in service_map:
        raise ValueError(f"Unknown service type: {service}. Valid types: {list(service_map.keys())}")
    
    return service_map[service]


# Example usage
if __name__ == "__main__":
    """
    Example usage of the Schwab endpoints configuration.
    """
    endpoints = SchwabEndpoints()

    print("=== Schwab API Endpoints Configuration ===")
    print(f"OAuth Base URL: {endpoints.OAUTH_BASE_URL}")
    print(f"Market Data Base URL: {endpoints.MARKET_DATA_BASE_URL}")
    print(f"Trading Base URL: {endpoints.TRADING_BASE_URL}")
    print()

    print("=== OAuth Endpoints ===")
    print(f"Authorization: {endpoints.oauth_authorize}")
    print(f"Token: {endpoints.oauth_token}")
    print()

    print("=== Market Data Endpoints ===")
    print(f"Quotes: {endpoints.quotes}")
    print(f"Quote for AAPL: {endpoints.quote_by_symbol('AAPL')}")
    print(f"Option Chains: {endpoints.option_chains}")
    print(f"Price History: {endpoints.price_history}")
    print(f"Market Hours: {endpoints.market_hours}")
    print(f"Instruments: {endpoints.instruments}")
    print()

    print("=== Trading Endpoints ===")
    print(f"Account Numbers: {endpoints.account_numbers}")
    print(f"Accounts: {endpoints.accounts}")
    print(f"Account by Number: {endpoints.account_by_number('12345678')}")
    print(f"Account Orders: {endpoints.account_orders('12345678')}")
    print(f"User Preferences: {endpoints.user_preferences}")
    print()

    print("=== Helper Functions ===")
    print(f"OAuth service base: {get_base_url_for_service('oauth')}")
    print(f"Market data service base: {get_base_url_for_service('market_data')}")
    print(f"Trading service base: {get_base_url_for_service('trading')}")
    print()
    print(f"Quotes endpoint: {get_endpoint_for_operation('quotes')}")
    print(f"User preferences endpoint: {get_endpoint_for_operation('user_preferences')}")
