# Configuration Reference

This document provides a comprehensive reference for all configuration options in the Schwab ELT ETL Pipeline.

## Configuration Files

The pipeline uses two configuration files:
- **`.env`** - Sensitive information (credentials, account numbers)
- **`config.yaml`** - Non-sensitive application settings

## Environment Variables (.env)

### Account Information
```bash
# Your Schwab account number (8 digits)
ACCNT_NUM=12345678
```

### Email Configuration
```bash
# Gmail SMTP credentials for notifications
EMAIL_USERNAME=your-email@gmail.com
EMAIL_PASSWORD=your-app-password

# Target email for system notifications
NOTIFICATION_EMAIL=admin@yourdomain.com
```

### Database Configuration
```bash
# SQL Server connection details
SQL_USERNAME=sa
SQL_PASSWORD=your_secure_password
SQL_HOST=localhost
```

## Application Configuration (config.yaml)

### Application Settings
```yaml
application:
  name: "schwab-elt-etl-pipeline"
  timezone: "US/Pacific"  # Your local timezone
  
  api:
    data_name: "MAIN_DATA"    # Schwab API name for market data
    trade_name: "MAIN_TRADE"  # Schwab API name for trading operations
```

### Token Management
```yaml
tokens:
  refresh_threshold: 60  # Refresh tokens when < 60 seconds remain
```

### Service-Specific Configuration

#### Streaming Service
```yaml
streaming:
  symbols:
    - "$SPX"  # Symbols to stream
  
  timing:
    start_time: "06:30"     # When to start streaming (market time)
    end_time: "13:00"       # When to stop streaming
    padding_minutes: 30     # Extra time before/after market hours
  
  reconnection:
    max_attempts: 5         # Maximum reconnection attempts
    delay_seconds: 30       # Delay between reconnection attempts
```

#### Option Chains Service
```yaml
chains:
  symbols:
    - "$SPX"
  
  frequencies:
    minute_1:
      interval_seconds: 60
      dte_range: [0, 7]     # Days to expiration range
      strike_count: 200
    
    minute_5:
      interval_seconds: 300
      dte_range: [0, 45]
      strike_count: 200
    
    minute_30:
      interval_seconds: 1800
      dte_range: [0, 180]
      strike_count: 200
  
  database:
    table_name: "SPX_CHAIN"
    schema: "CHAINS.dbo"
```

#### OHLC Service
```yaml
ohlc:
  symbols:
    - "SPY"
    - "QQQ"
    - "IWM"
  
  schedule:
    minute_data: "09:35"    # Daily collection time for minute data
    daily_data: "16:05"     # Daily collection time for daily data
  
  database:
    stored_procedure: "OPT.PYTHON.SP_PY_PROCESS_OHLC"
```

#### Transactions Service
```yaml
transactions:
  polling:
    interval_seconds: 30    # How often to check for new transactions
  
  database:
    json_table: "OPT.SCHWAB.JSON_TRANSACTIONS"
    orders_table: "OPT.PYTHON.Orders"
    stored_procedure: "OPT.PYTHON.SP_PY_PARSE_TRANSACTIONS"
```

#### Balances Service
```yaml
balances:
  schedule:
    update_time: "16:10"    # Daily balance update time
  
  database:
    table_name: "OPT.SCHWAB.BALANCES"
```

### Database Configuration
```yaml
database:
  connection:
    timeout_seconds: 30
    pool_size: 5
    max_overflow: 10
  
  retry:
    max_attempts: 3
    delay_seconds: 5
```

### Logging Configuration
```yaml
logging:
  level: "INFO"             # DEBUG, INFO, WARNING, ERROR, CRITICAL
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  
  handlers:
    console: true
    file: false             # Set to true to enable file logging
    email: true             # Email critical errors
```

### Email Notifications
```yaml
email:
  smtp:
    server: "smtp.gmail.com"
    port: 587
    use_tls: true
  
  notifications:
    critical_errors: true
    daily_summary: false
    service_status: true
```

## Environment-Specific Overrides

You can override configuration values using environment variables with the prefix `SCHWAB_`:

```bash
# Override the timezone
SCHWAB_APPLICATION_TIMEZONE="US/Eastern"

# Override streaming start time
SCHWAB_STREAMING_TIMING_START_TIME="06:00"

# Override database timeout
SCHWAB_DATABASE_CONNECTION_TIMEOUT_SECONDS=60
```

## Validation

The pipeline validates configuration on startup:

- **Required fields** must be present
- **Numeric values** must be within valid ranges
- **Time formats** must be valid (HH:MM)
- **Database connections** are tested
- **API credentials** are verified

## Security Best Practices

### Environment Variables
- Use strong, unique passwords
- Rotate credentials regularly
- Never commit `.env` files to version control
- Use environment-specific `.env` files for different deployments

### Configuration Files
- Keep `config.yaml` in version control (no secrets)
- Use different configurations for development/production
- Validate all user inputs
- Monitor configuration changes

## Common Configuration Patterns

### Development Environment
```yaml
# Shorter intervals for testing
chains:
  frequencies:
    minute_1:
      interval_seconds: 30  # Faster collection for testing

logging:
  level: "DEBUG"            # More verbose logging
```

### Production Environment
```yaml
# Optimized for stability
database:
  connection:
    pool_size: 10           # Larger connection pool
    timeout_seconds: 60     # Longer timeout

email:
  notifications:
    daily_summary: true     # Enable production monitoring
```

### High-Frequency Trading
```yaml
# Minimal latency configuration
streaming:
  timing:
    padding_minutes: 5      # Reduced padding

chains:
  frequencies:
    minute_1:
      interval_seconds: 15  # Very frequent updates
```

## Troubleshooting Configuration

### Common Issues
- **Invalid timezone**: Use standard timezone names (e.g., "US/Pacific")
- **Database connection failures**: Check credentials and network access
- **Email authentication**: Use Gmail App Passwords, not regular passwords
- **API rate limits**: Increase intervals if hitting rate limits

### Configuration Validation
```bash
# Test configuration loading
python -c "from tools.config import load_config; print('Configuration valid!')"

# Validate database connection
python -c "from tools.db import DB; DB().test_connection()"

# Test email configuration
python -c "from tools.emailer import test_email_config; test_email_config()"
```
