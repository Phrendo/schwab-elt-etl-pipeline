"""
Email Notification Module

This module provides email notification functionality for the Schwab API project.
It uses Gmail's SMTP service for sending notifications about system events,
errors, and status updates.

Features:
- Gmail SMTP integration (hardcoded for reliability)
- Simple interface for sending notifications
- Configurable recipient email address

Dependencies:
- smtplib: For SMTP email sending
- email.message: For email message construction
- os: For environment variable access

Note:
    Gmail SMTP server configuration is intentionally hardcoded for reliability.
    Email credentials and recipient address are configurable via environment variables.
"""

import smtplib
import os
from email.message import EmailMessage
from tools.config import get_config


def send_email(subject, body):
    """
    Send an email notification using Gmail SMTP or log to screen. If email fails, logs to screen.

    Sends an email with the specified subject and body to the configured
    recipient if all email configuration is present. If any email configuration
    is missing, logs the notification to the screen instead.

    Args:
        subject (str): The email subject line
        body (str): The email body content (plain text)

    Environment Variables Required:
        EMAIL_USERNAME: Gmail username for SMTP authentication
        EMAIL_PASSWORD: Gmail app password for SMTP authentication
        NOTIFICATION_EMAIL: Target email address for notifications

    Behavior:
        - If all email config is set: Sends email via Gmail SMTP
        - If any email config is missing: Logs notification to screen
        - If email sending fails: Logs notification to screen with error

    Note:
        Gmail SMTP configuration is hardcoded for reliability:
        - Server: smtp.gmail.com
        - Port: 587 (STARTTLS)
        - Authentication: Username/password from environment variables
    """
    # Gmail SMTP configuration
    SMTP_SERVER = 'smtp.gmail.com'  # Gmail SMTP server - kept hardcoded
    SMTP_PORT = 587  # STARTTLS port - kept hardcoded

    # Get email configuration from centralized config system
    config = get_config()
    email_config = config.get_email_config()

    email_username = email_config.get('username')
    email_password = email_config.get('password')
    recipient_email = email_config.get('notification_email')
    email_enabled = email_config.get('enabled', True)

    # Check if email is properly configured and enabled
    if not email_enabled or not recipient_email or not email_username or not email_password:
        print(f"EMAIL NOTIFICATION: {subject}")
        print(f"MESSAGE: {body}")

        missing_configs = []
        if not email_enabled:
            missing_configs.append("email disabled")
        if not recipient_email:
            missing_configs.append("NOTIFICATION_EMAIL")
        if not email_username:
            missing_configs.append("EMAIL_USERNAME")
        if not email_password:
            missing_configs.append("EMAIL_PASSWORD")

        print(f"({', '.join(missing_configs)} - logged to screen only)")
        return

    # Construct email message
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = email_username
    msg['To'] = recipient_email
    msg.set_content(body)

    # Send email using Gmail SMTP
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()  # Enable TLS encryption
            server.login(email_username, email_password)  # Authenticate with Gmail
            server.send_message(msg)  # Send the message
            print(f"Email sent to {recipient_email}")
    except Exception as e:
        # If email fails, log to screen as fallback
        print(f"EMAIL NOTIFICATION (failed to send): {subject}")
        print(f"MESSAGE: {body}")
        print(f"Error: {e}")

# Test function - uncomment to test email functionality
# if __name__ == "__main__":
#     send_email("Test Email", "This is a test email from your Schwab API project.")
