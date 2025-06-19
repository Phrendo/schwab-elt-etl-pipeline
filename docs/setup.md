# Setup Guide

This guide walks you through setting up the Schwab ELT ETL Pipeline from scratch.

## Prerequisites

### System Requirements
- **Python 3.8+** with pip
- **SQL Server** (local or remote instance)
- **Schwab API Access** (OAuth2 application registration)
- **Gmail Account** (optional, for email notifications)

### Schwab API Setup
1. Register for a Schwab Developer account at [developer.schwab.com](https://developer.schwab.com)
2. Create an OAuth2 application
3. Note your **Client ID** and **Client Secret**
4. Configure redirect URI (typically `https://localhost:8080/callback`)

## Installation Steps

### 1. Clone and Setup Environment

```bash
# Clone the repository
git clone <repository-url>
cd schwab-elt-etl-pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Create a `.env` file in the project root:

```bash
# Copy the example file
cp .env.example .env
```

Edit `.env` with your credentials:

```bash
# ========================================================================
# Schwab ELT ETL Pipeline - Environment Variables (Secrets Only)
# ========================================================================

# -------------------------------------------------------- ACCOUNT INFORMATION ------
# Your Schwab account number (8 digits)
ACCNT_NUM=12345678

# -------------------------------------------------------- EMAIL CREDENTIALS ------
# Gmail SMTP credentials for sending notifications
EMAIL_USERNAME=your-email@gmail.com
EMAIL_PASSWORD=your-app-password

# Target email address for system notifications
NOTIFICATION_EMAIL=notifications@yourdomain.com

# -------------------------------------------------------- DATABASE CREDENTIALS ------
# SQL Server connection credentials
SQL_USERNAME=your_db_user
SQL_PASSWORD=your_db_password
SQL_HOST=your_db_host
```

### 3. Configure Application Settings

The `config.yaml` file contains non-secret configuration. Review and adjust:

```yaml
application:
  name: "schwab-elt-etl-pipeline"
  timezone: "US/Pacific"  # Adjust to your timezone
  
  api:
    data_name: "MAIN_DATA"    # Your Schwab API name for data operations
    trade_name: "MAIN_TRADE"  # Your Schwab API name for trading operations
```

### 4. Database Setup

#### Create Database Schema
Run the SQL scripts to create the required database structure:

```bash
# Execute the database setup script
sqlcmd -S your_server -d your_database -i sql/opt.sql
```

#### Load Market Hours Data
```bash
# Load market hours into the database
python services/schwab_market_service.py
```

### 5. Initial Authentication

#### Generate Account Hash
```bash
# Store your Schwab account hash in the database
python scripts/schwab_hash.py
```

#### Perform Initial OAuth Flow
The first time you run any service, you'll need to complete OAuth authentication:

1. Run any service (e.g., `python services/schwab_balances_service.py`)
2. Follow the OAuth URL that appears in the console
3. Complete authentication in your browser
4. The system will store your tokens automatically

## Verification

### Test Database Connection
```bash
python -c "from tools.db import DB; db = DB(); print('Database connection successful!')"
```

### Test API Authentication
```bash
python -c "from tools.schwab import SchwabAPI; api = SchwabAPI('MAIN_DATA'); print('API authentication successful!')"
```

### Test Email Notifications (Optional)
```bash
python -c "from tools.emailer import send_email; send_email('Test Subject', 'Test message')"
```

## Next Steps

1. **[Configure Services](services.md)** - Set up individual data collection services
2. **[Database Schema](database.md)** - Understand the database structure
3. **[Running Operations](operations.md)** - Start collecting data

## Troubleshooting

### Common Issues

**Database Connection Errors**
- Verify SQL Server is running and accessible
- Check firewall settings for database port
- Confirm credentials in `.env` file

**API Authentication Failures**
- Verify Schwab API credentials
- Check if your OAuth application is approved
- Ensure redirect URI matches your configuration

**Email Notification Issues**
- Use Gmail App Passwords instead of regular passwords
- Enable 2-factor authentication on Gmail
- Check Gmail security settings

**Token Refresh Errors**
- Delete existing tokens from database and re-authenticate
- Verify system clock is accurate
- Check network connectivity to Schwab API

### Getting Help

1. Check the logs in the console output
2. Review the [Configuration Reference](configuration.md)
3. Consult the [API Documentation](api.md)
4. Check the [Operations Guide](operations.md) for monitoring tips

## Security Notes

- Never commit your `.env` file to version control
- Use strong passwords for database access
- Regularly rotate API credentials
- Monitor access logs for unusual activity
- Keep your system and dependencies updated
