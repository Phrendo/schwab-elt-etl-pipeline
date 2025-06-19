# Schwab API Integration

This document covers the integration with Charles Schwab's API, including authentication, endpoints, and data access patterns used throughout the pipeline.

## API Overview

### Three-Service Architecture

Schwab organizes their API into three distinct services:

1. **OAuth Service** (`https://api.schwabapi.com/v1`)
   - Authentication and token management
   - Authorization flows

2. **Market Data Service** (`https://api.schwabapi.com/marketdata/v1`)
   - Quotes and pricing data
   - Option chains
   - Market hours

3. **Trading Service** (`https://api.schwabapi.com/trader/v1`)
   - Account information
   - Order management
   - User preferences (includes streaming credentials)

## Authentication

### OAuth2 Flow

The pipeline uses OAuth2 with PKCE (Proof Key for Code Exchange) for secure authentication:

1. **Authorization Request**: User authorizes application
2. **Code Exchange**: Exchange authorization code for tokens
3. **Token Refresh**: Automatic refresh before expiration
4. **Token Storage**: Secure database storage

### Implementation

```python
from tools.schwab import SchwabAPI

# Initialize API client
api = SchwabAPI("MAIN_DATA")

# First-time authentication (interactive)
await api.authenticate()

# Subsequent calls use stored tokens automatically
quotes = await api.get_quotes(["$SPX"])
```

### Token Management

- **Access Tokens**: Valid for 30 minutes
- **Refresh Tokens**: Valid for 7 days
- **Automatic Refresh**: Handled by `tokens_service.py`
- **Database Storage**: Encrypted token storage in `OPT.SCHWAB.API`

## API Endpoints

### Market Data Endpoints

#### Get Quotes
```python
# Single symbol
quote = await api.get_quotes("$SPX")

# Multiple symbols
quotes = await api.get_quotes(["$SPX", "SPY", "QQQ"])
```

**Response Structure**:
```json
{
  "$SPX": {
    "symbol": "$SPX",
    "bid": 4150.25,
    "ask": 4150.50,
    "last": 4150.30,
    "mark": 4150.375,
    "bidSize": 100,
    "askSize": 100,
    "lastSize": 1,
    "totalVolume": 1234567
  }
}
```

#### Get Option Chains
```python
# Get SPX option chains
chains = await api.get_chains(
    symbol="$SPX",
    from_date=0,      # Days from today
    to_date=7,        # Days from today
    strike_count=200  # Number of strikes
)
```

**Response Structure**:
```json
{
  "callExpDateMap": {
    "2024-01-19:7": {
      "4150.0": [{
        "symbol": "SPXW_011924C4150",
        "bid": 25.50,
        "ask": 26.00,
        "last": 25.75,
        "delta": 0.52,
        "gamma": 0.003,
        "theta": -0.15,
        "vega": 0.08,
        "impliedVolatility": 0.18
      }]
    }
  },
  "putExpDateMap": { /* Similar structure */ }
}
```

#### Get Market Hours
```python
# Get market hours for specific date
hours = await api.get_market_hours("2024-01-19")
```

### Trading Endpoints

#### Get User Preferences
```python
# Includes streaming credentials
prefs = await api.get_user_preferences()
streamer_info = prefs.get('streamerInfo', {})
```

#### Get Account Information
```python
# Get account details
accounts = await api.get_accounts()
```

#### Get Transactions
```python
# Get recent transactions
transactions = await api.get_transactions(
    account_hash="your_account_hash",
    start_date="2024-01-01",
    end_date="2024-01-19"
)
```

## Rate Limiting

### Limits
- **120 requests per minute** for most endpoints
- **Streaming**: Separate connection limits
- **Burst capacity**: Short-term higher rates allowed

### Implementation
```python
from tools.decorators import rate_limit

@rate_limit(calls=120, period=60)
async def api_call():
    # Your API call here
    pass
```

### Best Practices
- Batch requests when possible
- Use streaming for real-time data
- Implement exponential backoff on rate limit errors
- Monitor usage patterns

## Error Handling

### Common Error Codes

| Code | Description | Action |
|------|-------------|--------|
| 401 | Unauthorized | Refresh token |
| 403 | Forbidden | Check permissions |
| 429 | Rate Limited | Implement backoff |
| 500 | Server Error | Retry with delay |

### Implementation
```python
from tools.decorators import retry, error_handler

@retry(max_attempts=3, delay=1.0)
@error_handler
async def robust_api_call():
    try:
        return await api.get_quotes("$SPX")
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            await api.refresh_token()
            return await api.get_quotes("$SPX")
        raise
```

## Streaming API

### WebSocket Connection
```python
from services.schwab_stream import SchwabStream

# Initialize streaming
stream = SchwabStream()

# Start streaming SPX data
await stream.start_streaming()
```

### Subscription Management
- **Level I**: Basic quotes
- **Options**: Option chain updates
- **News**: Market news feeds
- **Charts**: Real-time chart data

### Data Format
```json
{
  "service": "LEVELONE_FUTURES",
  "timestamp": 1642608000000,
  "command": "SUBS",
  "content": [{
    "key": "/ES",
    "1": 4150.25,  // Bid
    "2": 4150.50,  // Ask
    "3": 4150.30,  // Last
    "8": 1234567   // Volume
  }]
}
```

## Configuration

### Environment Variables
```bash
# API Configuration (in config.yaml)
SCHWAB_API_DATA_NAME="MAIN_DATA"
SCHWAB_API_TRADE_NAME="MAIN_TRADE"

# Account Information (in .env)
ACCNT_NUM=12345678
```

### Endpoint Configuration
```python
from tools.schwab_endpoints import SCHWAB_ENDPOINTS

# Access configured endpoints
quotes_url = SCHWAB_ENDPOINTS.quotes
chains_url = SCHWAB_ENDPOINTS.option_chains
```

## Data Processing Patterns

### Real-time Processing
```python
async def process_streaming_data(data):
    # Parse incoming data
    parsed = parse_stream_data(data)
    
    # Store in database
    await db.store_streaming_data(parsed)
    
    # Write to parquet for analysis
    parquet_writer.write_batch(parsed)
```

### Batch Processing
```python
async def collect_option_chains():
    # Get current chains
    chains = await api.get_chains("$SPX", 0, 7, 200)
    
    # Process and normalize
    normalized = normalize_chains_data(chains)
    
    # Store in database
    await db.store_chains_data(normalized)
```

## Security Considerations

### Token Security
- Store tokens encrypted in database
- Use secure connection strings
- Rotate credentials regularly
- Monitor for unusual access patterns

### API Security
- Validate all input parameters
- Use parameterized database queries
- Implement proper error handling
- Log security events

### Network Security
- Use HTTPS for all API calls
- Validate SSL certificates
- Implement connection timeouts
- Monitor network traffic

## Monitoring and Alerting

### Key Metrics
- API response times
- Error rates by endpoint
- Token refresh frequency
- Rate limit utilization

### Alerts
- Authentication failures
- Repeated API errors
- Rate limit violations
- Unusual data patterns

### Implementation
```python
from tools.emailer import send_email

async def monitored_api_call():
    try:
        start_time = time.time()
        result = await api.get_quotes("$SPX")

        # Log performance
        duration = time.time() - start_time
        logger.info(f"API call completed in {duration:.2f}s")

        return result
    except Exception as e:
        # Send alert
        send_email("API Error", str(e))
        raise
```

## Testing

### Unit Tests
```python
import pytest
from unittest.mock import AsyncMock

@pytest.mark.asyncio
async def test_get_quotes():
    api = SchwabAPI("TEST")
    api.client.get = AsyncMock(return_value=mock_response)
    
    result = await api.get_quotes("$SPX")
    assert result["$SPX"]["symbol"] == "$SPX"
```

### Integration Tests
```python
@pytest.mark.integration
async def test_full_auth_flow():
    api = SchwabAPI("TEST")
    
    # Test authentication
    await api.authenticate()
    
    # Test API call
    quotes = await api.get_quotes("$SPX")
    assert quotes is not None
```

For detailed configuration, see [Configuration Reference](configuration.md).
For service implementation examples, see [Services Overview](services.md).
