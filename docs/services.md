# Services Overview

The Schwab ELT ETL Pipeline consists of several data collection services that gather different types of financial data from the Schwab API.

## Available Services

### 1. Streaming Service (`schwab_stream.py`)
Real-time WebSocket streaming of SPX options and underlying price data.

**Data Flow**:
- **Redis Cache**: Live quotes stored via `set_latest_quote()` for real-time access
- **Parquet Files**: All streaming data written to Parquet format for analysis
- **No Database**: Streaming data does NOT go to SQL database

**What it streams**:
- SPX weekly options (generated around current price ±100 points, 5-point intervals)
- SPX underlying price ticks
- Runs from market open until 1:00 PM Pacific Writes data to Redis cache and Parquet files (not database).

### 2. Option Chains Service (`schwab_chains_service.py`)
Scheduled collection of SPX option chains data at multiple frequencies (1-min, 5-min, 30-min intervals).

### 3. OHLC Service (`schwab_ohlc_service.py`)
Historical and real-time OHLC (Open, High, Low, Close) price data collection for configured symbols.

### 4. Transactions Service (`schwab_transactions_service.py`)
Continuous monitoring and processing of account transactions with raw JSON storage and structured data processing.

### 5. Balances Service (`schwab_balances_service.py`)
Account balance and position monitoring with scheduled updates.

### 6. Market Service (`schwab_market_service.py`)
Market hours and trading session management - provides foundation data for other services' scheduling.

### 7. Tokens Service (`tokens_service.py`)
OAuth2 token management and automatic refresh before expiration.

### 8. Stream Controller (`schwab_stream_controller.py`)
Manages the lifecycle of streaming services - starts at market open, monitors health, terminates at market close.

### 9. Stream Monitor (`schwab_stream_monitor.py`)
Monitors streaming service health and performance with automatic restart capabilities.

## Running Services

### Manual Execution
```bash
python services/schwab_stream.py
python services/schwab_chains_service.py
python services/schwab_ohlc_service.py
python services/schwab_transactions_service.py
python services/schwab_balances_service.py
python services/schwab_market_service.py
python services/tokens_service.py
```

## Configuration

Services are configured via `config.yaml` and `.env` files. Each service has its own configuration section in `config.yaml` with specific settings for timing, intervals, and behavior.
