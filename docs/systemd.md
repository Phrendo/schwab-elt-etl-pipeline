# Systemd Integration

This guide covers setting up and managing the Schwab ELT ETL Pipeline services using systemd for production deployment.

## Service Overview

The pipeline consists of multiple systemd services that work together:

- **schwab-tokens** - Token management and refresh
- **schwab-stream** - Real-time data streaming
- **schwab-chains** - Option chains collection
- **schwab-ohlc** - OHLC data collection
- **schwab-transactions** - Transaction monitoring
- **schwab-balances** - Account balance updates
- **schwab-stream-controller** - Stream lifecycle management
- **schwab-stream-monitor** - Stream health monitoring

## Service Files

### 1. Token Service (`/etc/systemd/system/schwab-tokens.service`)

```ini
[Unit]
Description=Schwab API Token Management Service
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=bobby
Group=bobby
WorkingDirectory=/opt/schwab-elt-etl-pipeline
Environment=PYTHONPATH=/opt/schwab-elt-etl-pipeline
ExecStart=/opt/schwab-elt-etl-pipeline/.venv/bin/python services/tokens_service.py
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
Environment="PYTHONUNBUFFERED=1"

[Install]
WantedBy=multi-user.target
```

### 2. Streaming Service (`/etc/systemd/system/schwab-stream.service`)

```ini
[Unit]
Description=Schwab Real-time Data Streaming Service
After=network.target schwab-tokens.service
Wants=network-online.target

[Service]
Type=simple
User=bobby
Group=bobby
WorkingDirectory=/opt/schwab-elt-etl-pipeline
Environment=PYTHONPATH=/opt/schwab-elt-etl-pipeline
ExecStart=/opt/schwab-elt-etl-pipeline/.venv/bin/python services/schwab_stream.py
Restart=always
RestartSec=60
StandardOutput=journal
StandardError=journal
Environment="PYTHONUNBUFFERED=1"

[Install]
WantedBy=multi-user.target
```

### 3. Option Chains Service (`/etc/systemd/system/schwab-chains.service`)

```ini
[Unit]
Description=Schwab Option Chains Collection Service
After=network.target schwab-tokens.service
Wants=network-online.target

[Service]
Type=simple
User=bobby
Group=bobby
WorkingDirectory=/opt/schwab-elt-etl-pipeline
Environment=PYTHONPATH=/opt/schwab-elt-etl-pipeline
ExecStart=/opt/schwab-elt-etl-pipeline/.venv/bin/python services/schwab_chains_service.py
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
Environment="PYTHONUNBUFFERED=1"

[Install]
WantedBy=multi-user.target
```

### 4. OHLC Service (`/etc/systemd/system/schwab-ohlc.service`)

```ini
[Unit]
Description=Schwab OHLC Data Collection Service
After=network.target schwab-tokens.service
Wants=network-online.target

[Service]
Type=simple
User=bobby
Group=bobby
WorkingDirectory=/opt/schwab-elt-etl-pipeline
Environment=PYTHONPATH=/opt/schwab-elt-etl-pipeline
ExecStart=/opt/schwab-elt-etl-pipeline/.venv/bin/python services/schwab_ohlc_service.py
Restart=always
RestartSec=60
StandardOutput=journal
StandardError=journal
Environment="PYTHONUNBUFFERED=1"

[Install]
WantedBy=multi-user.target
```

### 5. Transactions Service (`/etc/systemd/system/schwab-transactions.service`)

```ini
[Unit]
Description=Schwab Transactions Monitoring Service
After=network.target schwab-tokens.service
Wants=network-online.target

[Service]
Type=simple
User=bobby
Group=bobby
WorkingDirectory=/opt/schwab-elt-etl-pipeline
Environment=PYTHONPATH=/opt/schwab-elt-etl-pipeline
ExecStart=/opt/schwab-elt-etl-pipeline/.venv/bin/python services/schwab_transactions_service.py
Restart=always
RestartSec=60
StandardOutput=journal
StandardError=journal
Environment="PYTHONUNBUFFERED=1"s

[Install]
WantedBy=multi-user.target
```

### 6. Balances Service (`/etc/systemd/system/schwab-balances.service`)

```ini
[Unit]
Description=Schwab Account Balances Service
After=network.target schwab-tokens.service
Wants=network-online.target

[Service]
Type=simple
User=bobby
Group=bobby
WorkingDirectory=/opt/schwab-elt-etl-pipeline
Environment=PYTHONPATH=/opt/schwab-elt-etl-pipeline
ExecStart=/opt/schwab-elt-etl-pipeline/.venv/bin/python services/schwab_balances_service.py
Restart=always
RestartSec=60
StandardOutput=journal
StandardError=journal
Environment="PYTHONUNBUFFERED=1"

[Install]
WantedBy=multi-user.target
```

## Installation

### 1. Create Service User
```bash
# Create dedicated user for services
sudo useradd -r -s /bin/false schwab
sudo usermod -a -G schwab schwab

# Set up directories
sudo mkdir -p /opt/schwab-elt-etl-pipeline
sudo chown schwab:schwab /opt/schwab-elt-etl-pipeline
```

### 2. Deploy Application
```bash
# Copy application files
sudo cp -r /path/to/schwab-elt-etl-pipeline/* /opt/schwab-elt-etl-pipeline/
sudo chown -R schwab:schwab /opt/schwab-elt-etl-pipeline

# Set up virtual environment
sudo -u schwab python3 -m venv /opt/schwab-elt-etl-pipeline/.venv
sudo -u schwab /opt/schwab-elt-etl-pipeline/.venv/bin/pip install -r /opt/schwab-elt-etl-pipeline/requirements.txt
```

### 3. Install Service Files
```bash
# Copy service files
sudo cp systemd/*.service /etc/systemd/system/

# Reload systemd
sudo systemctl daemon-reload

# Enable services
sudo systemctl enable schwab-tokens
sudo systemctl enable schwab-stream
sudo systemctl enable schwab-chains
sudo systemctl enable schwab-ohlc
sudo systemctl enable schwab-transactions
sudo systemctl enable schwab-balances
```

### 4. Configure Environment
```bash
# Create environment file
sudo cp .env /opt/schwab-elt-etl-pipeline/.env
sudo chown schwab:schwab /opt/schwab-elt-etl-pipeline/.env
sudo chmod 600 /opt/schwab-elt-etl-pipeline/.env

# Create log directory
sudo mkdir -p /opt/schwab-elt-etl-pipeline/logs
sudo chown schwab:schwab /opt/schwab-elt-etl-pipeline/logs
```

## Service Management

### Starting Services
```bash
# Start in dependency order
sudo systemctl start schwab-tokens
sudo systemctl start schwab-stream
sudo systemctl start schwab-chains
sudo systemctl start schwab-ohlc
sudo systemctl start schwab-transactions
sudo systemctl start schwab-balances
```

### Stopping Services
```bash
# Stop all services
sudo systemctl stop schwab-balances
sudo systemctl stop schwab-transactions
sudo systemctl stop schwab-ohlc
sudo systemctl stop schwab-chains
sudo systemctl stop schwab-stream
sudo systemctl stop schwab-tokens
```

### Service Status
```bash
# Check individual service
sudo systemctl status schwab-stream

# Check all services
sudo systemctl status schwab-*

# Get detailed status
sudo systemctl show schwab-stream
```

### Service Logs
```bash
# View real-time logs
sudo journalctl -u schwab-stream -f

# View logs for all services
sudo journalctl -u schwab-* -f

# View logs for specific time period
sudo journalctl -u schwab-stream --since "2024-01-19 09:00:00"

# Export logs
sudo journalctl -u schwab-stream --since yesterday > stream_logs.txt
```

## Troubleshooting

### Common Issues

#### Service Won't Start
```bash
# Check service status
sudo systemctl status schwab-stream

# Check journal for errors
sudo journalctl -u schwab-stream -n 50

# Verify configuration
sudo -u schwab /opt/schwab-elt-etl-pipeline/.venv/bin/python -c "from tools.config import load_config; load_config()"
```

For operational procedures, see [Operations Guide](operations.md).
