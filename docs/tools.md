# Tools & Modules

The `tools/` directory contains core reusable modules that provide essential functionality for the entire pipeline. These modules handle database operations, API interactions, configuration management, and utility functions.

## Core Modules

### 1. Database Module (`db.py`)

**Purpose**: Centralized database operations with connection pooling and transaction management.

**Key Features**:
- SQL Server connection management with pooling
- Automatic transaction handling
- Parameterized query execution
- Token storage and retrieval
- Connection health monitoring

**Main Classes**:
- `DB`: Primary database interface

**Common Usage**:
```python
from tools.db import DB

# Initialize database connection
db = DB()

# Execute a query
results = db.execute_query(
    "SELECT * FROM users WHERE active = :active", 
    {"active": True}
)

# Execute an update
db.execute_non_query(
    "UPDATE users SET last_login = :timestamp WHERE id = :user_id",
    {"timestamp": datetime.now(), "user_id": 123}
)

# Token management
token_data = db.get_token("MAIN_DATA")
db.store_token("MAIN_DATA", token_data)
```

### 2. Schwab API Client (`schwab.py`)

**Purpose**: Unified interface for Schwab API authentication and data operations.

**Key Features**:
- OAuth2 authentication flow
- Automatic token refresh
- Rate limiting and retry logic
- Support for multiple API configurations
- Email notifications for failures

**Main Classes**:
- `SchwabAPI`: Unified API client

**Common Usage**:
```python
from tools.schwab import SchwabAPI

# Initialize API client
api = SchwabAPI("MAIN_DATA")

# Get quotes
quotes = await api.get_quotes(["$SPX", "SPY"])

# Get option chains
chains = await api.get_chains("$SPX", from_date=0, to_date=7, strike_count=200)

# Get user preferences (includes streaming info)
prefs = await api.get_user_preferences()
```

### 3. Configuration Management (`config.py`)

**Purpose**: Centralized configuration loading and validation.

**Key Features**:
- YAML configuration file parsing
- Environment variable integration
- Configuration validation
- Type conversion and defaults

**Common Usage**:
```python
from tools.config import load_config

# Load configuration
config = load_config()

# Access configuration values - Note: there are still some timezone hardcodes set to pacific TZ - 06:30 and 13:00
timezone = config['application']['timezone']
api_name = config['application']['api']['data_name']
```

### 4. Email Notifications (`emailer.py`)

**Purpose**: Email notification system for alerts and monitoring.

**Key Features**:
- Gmail SMTP integration
- HTML and plain text emails
- Error notification templates
- Configurable recipients

**Common Usage**:
```python
from tools.emailer import send_email

# Send basic email
send_email("Subject", "Message body")

# Send error notification
send_email("Service Error", "Error details here")
```

### 5. Logging Configuration (`logging_config.py`)

**Purpose**: Standardized logging setup across all services.

**Key Features**:
- Consistent log formatting
- Multiple output handlers
- Log level configuration
- Service-specific loggers

**Common Usage**:
```python
from tools.logging_config import setup_logging
import logging

# Setup logging for a service
setup_logging("schwab_stream")
logger = logging.getLogger(__name__)

logger.info("Service started")
logger.error("An error occurred", exc_info=True)
```

### 6. Decorators (`decorators.py`)

**Purpose**: Reusable decorators for common functionality.

**Key Features**:
- Retry logic with exponential backoff
- Performance timing
- Error handling and logging
- Rate limiting

**Common Usage**:
```python
from tools.decorators import retry, timing, error_handler

@retry(max_attempts=3, delay=1.0)
@timing
@error_handler
async def api_call():
    # Your API call here
    pass
```

### 7. Schwab Endpoints (`schwab_endpoints.py`)

**Purpose**: Centralized definition of all Schwab API endpoints.

**Key Features**:
- Three-service architecture (OAuth, Market Data, Trading)
- Type-safe endpoint definitions
- Environment variable overrides
- Comprehensive documentation

**Common Usage**:
```python
from tools.schwab_endpoints import SCHWAB_ENDPOINTS

# Get endpoint URLs
auth_url = SCHWAB_ENDPOINTS.oauth_authorize
quotes_url = SCHWAB_ENDPOINTS.quotes
user_prefs_url = SCHWAB_ENDPOINTS.user_preferences
```

### 8. Utilities (`utils.py`)

**Purpose**: Common utility functions used across the pipeline.

**Key Features**:
- Date/time manipulation
- Data validation
- String formatting
- File operations

**Common Usage**:
```python
from tools.utils import (
    is_market_hours,
    format_timestamp,
    validate_symbol,
    safe_float_conversion
)

# Check if current time is during market hours
if is_market_hours():
    # Perform market operations
    pass

# Format timestamp for database
formatted_time = format_timestamp(datetime.now())
```

### 9. Parquet Writer (`parquet_writer.py`)

**Purpose**: High-performance storage for streaming market data.

**Key Features**:
- Writes streaming data to Parquet format
- Used by streaming service for all market data
- Efficient columnar storage with compression

**Usage in Streaming**:
```python
from tools.parquet_writer import ParquetWriter

# Initialize writer (done in streaming service)
writer = ParquetWriter()

# Write individual records
writer.write(record_dict)
writer.close()
```

### 10. Redis Cache (`redis_cache.py`)

**Purpose**: Caching layer for real-time streaming data.

**Key Features**:
- Stores latest quotes from streaming service
- Used by streaming service via `set_latest_quote()`
- Provides fast access to current market data

**Usage in Streaming**:
```python
from tools.redis_cache import set_latest_quote

# Store latest quote (used by streaming service)
set_latest_quote(symbol, json_data)
```

## Module Dependencies

```
config.py (foundation)
    ↓
logging_config.py
    ↓
db.py ←→ schwab.py
    ↓         ↓
emailer.py   schwab_endpoints.py
    ↓         ↓
decorators.py utils.py
    ↓         ↓
parquet_writer.py redis_cache.py
```

## Design Principles

### 1. Single Responsibility
Each module has a clear, focused purpose:
- `db.py` handles only database operations
- `schwab.py` handles only API interactions
- `emailer.py` handles only email notifications

### 2. Configuration-Driven
All modules use external configuration:
- Environment variables for secrets
- YAML files for application settings
- No hardcoded values in production code

### 3. Error Resilience
All modules implement robust error handling:
- Automatic retry with exponential backoff
- Graceful degradation on failures
- Comprehensive error logging and notifications

### 4. Testability
Modules are designed for easy testing:
- Dependency injection where appropriate
- Clear interfaces and contracts
- Minimal external dependencies

### 5. Performance
Optimized for production use:
- Connection pooling for database operations
- Async/await for I/O operations
- Efficient data structures and algorithms

## Best Practices

### Import Patterns
```python
# Preferred: Import specific functions/classes
from tools.db import DB
from tools.schwab import SchwabAPI

# Avoid: Wildcard imports
from tools.db import *  # Don't do this
```

### Error Handling
```python
from tools.decorators import error_handler
from tools.emailer import send_email

@error_handler
async def service_function():
    try:
        # Your code here
        pass
    except Exception as e:
        send_email("Service Error", str(e))
        raise
```

### Configuration Access
```python
from tools.config import load_config

# Load once at module level
config = load_config()
API_NAME = config['application']['api']['data_name']

# Use throughout the module
def some_function():
    return API_NAME
```

### Logging
```python
from tools.logging_config import setup_logging
import logging

# Setup at module level
setup_logging("my_service")
logger = logging.getLogger(__name__)

# Use throughout the module
def some_function():
    logger.info("Function called")
```

## Testing

Each module includes comprehensive tests:
```bash
# Run tests for specific modules
python -m pytest tests/test_db.py
python -m pytest tests/test_schwab.py

# Run all tool tests
python -m pytest tests/tools/
```

For integration examples, see the [Services Documentation](services.md).
