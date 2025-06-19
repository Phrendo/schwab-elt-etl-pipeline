# Operations Guide

This guide covers running, monitoring, and troubleshooting the Schwab ELT ETL Pipeline in production environments.

## Starting the Pipeline

### Prerequisites Check
Before starting services, ensure:
```bash
# Database connectivity
python -c "from tools.db import DB; DB().test_connection()"

# API authentication
python -c "from tools.schwab import SchwabAPI; SchwabAPI('MAIN_DATA').test_connection()"

# Configuration validation
python -c "from tools.config import load_config; load_config()"
```

### Manual Service Startup

#### Start Core Services
```bash
# Start token management (run first)
python services/tokens_service.py &

# Start data collection services
python services/schwab_stream.py &
python services/schwab_chains_service.py &
python services/schwab_ohlc_service.py &
python services/schwab_transactions_service.py &
python services/schwab_balances_service.py &
```

#### Start with Logging
```bash
# Run with output logging
python services/schwab_stream.py > logs/stream.log 2>&1 &
python services/schwab_chains_service.py > logs/chains.log 2>&1 &
```

### Systemd Service Management

#### Start All Services
```bash
sudo systemctl start schwab-tokens
sudo systemctl start schwab-stream
sudo systemctl start schwab-chains
sudo systemctl start schwab-ohlc
sudo systemctl start schwab-transactions
sudo systemctl start schwab-balances
```

#### Enable Auto-Start
```bash
sudo systemctl enable schwab-*
```

#### Service Status
```bash
# Check individual service
sudo systemctl status schwab-stream

# Check all services
sudo systemctl status schwab-*
```

## Monitoring

### Service Health Monitoring

#### Check Running Processes
```bash
# List all schwab processes
ps aux | grep schwab

# Check specific service
ps aux | grep schwab_stream
```

#### Monitor Resource Usage
```bash
# CPU and memory usage
top -p $(pgrep -f schwab)

# Detailed process information
htop -p $(pgrep -f schwab | tr '\n' ',')
```

### Log Monitoring

#### Real-time Log Viewing
```bash
# Systemd logs
sudo journalctl -u schwab-stream -f
sudo journalctl -u schwab-* -f

# File logs (if configured)
tail -f logs/stream.log
tail -f logs/chains.log
```

#### Log Analysis
```bash
# Search for errors
sudo journalctl -u schwab-stream | grep ERROR

# Check last 100 lines
sudo journalctl -u schwab-stream -n 100

# Filter by time range
sudo journalctl -u schwab-stream --since "2024-01-19 09:00:00"
```

### Database Monitoring

#### Check Data Collection
```sql
-- Recent streaming data
SELECT TOP 10 * FROM streaming_data 
ORDER BY timestamp DESC;

-- Option chains collection status
SELECT 
    collection_frequency,
    COUNT(*) as records,
    MAX(collected_at) as last_collection
FROM CHAINS.dbo.SPX_CHAIN 
WHERE collected_at >= DATEADD(hour, -1, GETDATE())
GROUP BY collection_frequency;

-- Transaction processing status
SELECT 
    COUNT(*) as total_transactions,
    MAX(created_at) as last_transaction
FROM OPT.SCHWAB.JSON_TRANSACTIONS 
WHERE created_at >= DATEADD(hour, -1, GETDATE());
```

#### Performance Monitoring
```sql
-- Database connection count
SELECT 
    DB_NAME(database_id) as database_name,
    COUNT(*) as connection_count
FROM sys.dm_exec_sessions 
WHERE is_user_process = 1
GROUP BY database_id;

-- Long-running queries
SELECT 
    session_id,
    start_time,
    status,
    command,
    SUBSTRING(text, 1, 100) as query_text
FROM sys.dm_exec_requests 
CROSS APPLY sys.dm_exec_sql_text(sql_handle)
WHERE session_id > 50;
```

### API Monitoring

#### Token Status
```python
from tools.db import DB

db = DB()
tokens = db.execute_query("""
    SELECT api_name, expires_at, 
           DATEDIFF(minute, GETDATE(), expires_at) as minutes_remaining
    FROM OPT.SCHWAB.API
""")

for token in tokens:
    print(f"{token['api_name']}: {token['minutes_remaining']} minutes remaining")
```

#### Rate Limit Monitoring
```bash
# Check for rate limit errors in logs
sudo journalctl -u schwab-* | grep "429\|rate.limit"

# Monitor API response times
grep "API call completed" logs/*.log | tail -20
```

## Troubleshooting

### Common Issues

#### Service Won't Start
```bash
# Check service status
sudo systemctl status schwab-stream

# Check for configuration errors
python -c "from tools.config import load_config; load_config()"

# Verify database connection
python -c "from tools.db import DB; DB().test_connection()"

# Check file permissions
ls -la services/schwab_stream.py
```

#### Authentication Failures
```bash
# Check token expiration
python -c "
from tools.db import DB
import datetime
db = DB()
tokens = db.execute_query('SELECT * FROM OPT.SCHWAB.API')
for token in tokens:
    expires = token['expires_at']
    now = datetime.datetime.now()
    print(f'{token[\"api_name\"]}: expires {expires}, now {now}')
"

# Force token refresh
python -c "
from tools.schwab import SchwabAPI
api = SchwabAPI('MAIN_DATA')
api.refresh_token()
"
```

#### Database Connection Issues
```bash
# Test database connectivity
sqlcmd -S $SQL_HOST -U $SQL_USERNAME -P $SQL_PASSWORD -Q "SELECT GETDATE()"

# Check connection string
python -c "
from tools.db import DB
db = DB()
print('Database connection successful')
"

# Verify environment variables
echo $SQL_HOST $SQL_USERNAME
```

#### High Memory Usage
```bash
# Check memory usage by service
ps aux --sort=-%mem | grep schwab

# Monitor memory over time
while true; do
    ps aux | grep schwab_stream | awk '{print $4, $6}'
    sleep 60
done
```

### Performance Issues

#### Slow Database Queries
```sql
-- Find slow queries
SELECT TOP 10
    total_elapsed_time/execution_count as avg_time,
    execution_count,
    SUBSTRING(text, 1, 200) as query_text
FROM sys.dm_exec_query_stats 
CROSS APPLY sys.dm_exec_sql_text(sql_handle)
ORDER BY avg_time DESC;

-- Check index usage
SELECT 
    object_name(object_id) as table_name,
    name as index_name,
    user_seeks,
    user_scans,
    user_lookups
FROM sys.dm_db_index_usage_stats
WHERE database_id = DB_ID();
```

#### API Rate Limiting
```python
# Monitor API call frequency
import time
from collections import defaultdict

call_times = defaultdict(list)

def track_api_call(endpoint):
    now = time.time()
    call_times[endpoint].append(now)
    
    # Remove calls older than 1 minute
    cutoff = now - 60
    call_times[endpoint] = [t for t in call_times[endpoint] if t > cutoff]
    
    print(f"{endpoint}: {len(call_times[endpoint])} calls in last minute")
```

### Recovery Procedures

#### Service Recovery
```bash
# Restart failed service
sudo systemctl restart schwab-stream

# Force kill and restart
sudo pkill -f schwab_stream
sleep 5
sudo systemctl start schwab-stream
```

#### Database Recovery
```bash
# Check database integrity
sqlcmd -S $SQL_HOST -U $SQL_USERNAME -P $SQL_PASSWORD -Q "DBCC CHECKDB"

# Rebuild indexes if needed
sqlcmd -S $SQL_HOST -U $SQL_USERNAME -P $SQL_PASSWORD -Q "
ALTER INDEX ALL ON CHAINS.dbo.SPX_CHAIN REBUILD
"
```

#### Token Recovery
```python
# Clear and re-authenticate
from tools.db import DB
from tools.schwab import SchwabAPI

# Clear existing tokens
db = DB()
db.execute_non_query("DELETE FROM OPT.SCHWAB.API WHERE api_name = 'MAIN_DATA'")

# Re-authenticate
api = SchwabAPI('MAIN_DATA')
api.authenticate()  # Follow OAuth flow
```

## Maintenance

### Daily Tasks
```bash
# Check service status
sudo systemctl status schwab-*

# Review error logs
sudo journalctl -u schwab-* --since yesterday | grep ERROR

# Verify data collection
python scripts/daily_health_check.py
```

### Weekly Tasks
```bash
# Update database statistics
sqlcmd -S $SQL_HOST -U $SQL_USERNAME -P $SQL_PASSWORD -Q "
EXEC sp_updatestats
"

# Clean old log files
find logs/ -name "*.log" -mtime +7 -delete

# Review performance metrics
python scripts/weekly_performance_report.py
```

### Monthly Tasks
```bash
# Archive old data
python scripts/archive_old_data.py

# Review and optimize database indexes
python scripts/index_optimization.py

# Update system dependencies
pip install --upgrade -r requirements.txt
```

## Alerting

### Email Notifications
The pipeline automatically sends email alerts for:
- Service failures
- Authentication errors
- Database connection issues
- Critical data collection failures

### Custom Alerts
```python
from tools.emailer import send_email

def check_data_freshness():
    from tools.db import DB
    db = DB()
    
    # Check if data is recent
    result = db.execute_query("""
        SELECT MAX(collected_at) as last_collection
        FROM CHAINS.dbo.SPX_CHAIN
        WHERE collected_at >= DATEADD(minute, -10, GETDATE())
    """)
    
    if not result or not result[0]['last_collection']:
        send_email(
            "Data Collection Alert",
            "No option chains data collected in last 10 minutes"
        )
```

## Backup and Disaster Recovery

### Data Backup
```bash
# Database backup
sqlcmd -S $SQL_HOST -U $SQL_USERNAME -P $SQL_PASSWORD -Q "
BACKUP DATABASE YourDatabase 
TO DISK = 'C:\Backups\schwab_pipeline_$(date +%Y%m%d).bak'
WITH COMPRESSION
"

# Configuration backup
tar -czf config_backup_$(date +%Y%m%d).tar.gz .env config.yaml
```

### Service Recovery
```bash
# Create service recovery script
cat > recovery.sh << 'EOF'
#!/bin/bash
sudo systemctl stop schwab-*
sleep 10
sudo systemctl start schwab-tokens
sleep 30
sudo systemctl start schwab-stream schwab-chains schwab-ohlc
sudo systemctl start schwab-transactions schwab-balances
EOF

chmod +x recovery.sh
```

For detailed systemd configuration, see [Systemd Integration](systemd.md).
