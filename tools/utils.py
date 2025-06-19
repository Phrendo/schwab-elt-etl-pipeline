"""
Utility Functions Module

This module provides common utility functions for date/time conversion and parsing
operations used throughout the Schwab API project.

Features:
- Date/time parsing from various string formats
- Timezone conversion to Pacific Time
- Epoch timestamp conversion utilities

Dependencies:
- datetime: For date/time operations
- zoneinfo: For timezone handling (Python 3.9+)
"""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Union


def parse_date(date_string: str) -> datetime:
    """
    Parse a date string into a datetime object.

    Handles various date string formats commonly returned by the Schwab API,
    including ISO 8601 formats with and without timezone information.

    Args:
        date_string (str): Date string to parse (e.g., '2024-12-18T09:30:00-05:00')

    Returns:
        datetime: Parsed datetime object with timezone information

    Raises:
        ValueError: If the date string format is not recognized

    Example:
        dt = parse_date('2024-12-18T09:30:00-05:00')
        print(dt)  # 2024-12-18 09:30:00-05:00

    Supported Formats:
        - ISO 8601 with timezone: '2024-12-18T09:30:00-05:00'
        - ISO 8601 with Z (UTC): '2024-12-18T09:30:00Z'
        - ISO 8601 without timezone: '2024-12-18T09:30:00'
        - Date only: '2024-12-18'
    """
    # Remove any whitespace
    date_string = date_string.strip()
    
    # List of common date formats to try
    formats = [
        '%Y-%m-%dT%H:%M:%S%z',      # ISO 8601 with timezone offset
        '%Y-%m-%dT%H:%M:%S.%f%z',   # ISO 8601 with microseconds and timezone
        '%Y-%m-%dT%H:%M:%SZ',       # ISO 8601 with Z (UTC)
        '%Y-%m-%dT%H:%M:%S.%fZ',    # ISO 8601 with microseconds and Z
        '%Y-%m-%dT%H:%M:%S',        # ISO 8601 without timezone
        '%Y-%m-%dT%H:%M:%S.%f',     # ISO 8601 with microseconds, no timezone
        '%Y-%m-%d %H:%M:%S',        # Standard datetime format
        '%Y-%m-%d',                 # Date only
    ]
    
    # Handle 'Z' suffix by replacing with '+00:00'
    if date_string.endswith('Z'):
        date_string = date_string[:-1] + '+00:00'
    
    # Try each format
    for fmt in formats:
        try:
            dt = datetime.strptime(date_string, fmt)
            
            # If no timezone info, assume UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            
            return dt
        except ValueError:
            continue
    
    # If no format worked, raise an error
    raise ValueError(f"Unable to parse date string: '{date_string}'. "
                   f"Supported formats include ISO 8601 variants and standard datetime formats.")


def convert_to_pacific_time(dt: Union[datetime, str]) -> str:
    """
    Convert a datetime object or string to Pacific Time and return as formatted string.

    Takes a datetime object or date string and converts it to US/Pacific timezone,
    returning a formatted string suitable for database storage or display.

    Args:
        dt (Union[datetime, str]): Datetime object or date string to convert

    Returns:
        str: Formatted datetime string in Pacific Time (YYYY-MM-DD HH:MM:SS)

    Example:
        # From datetime object
        from datetime import datetime, timezone
        utc_dt = datetime(2024, 12, 18, 14, 30, 0, tzinfo=timezone.utc)
        pacific_str = convert_to_pacific_time(utc_dt)
        print(pacific_str)  # '2024-12-18 06:30:00'

        # From string
        pacific_str = convert_to_pacific_time('2024-12-18T14:30:00Z')
        print(pacific_str)  # '2024-12-18 06:30:00'

    Note:
        If input is a string, it will be parsed using parse_date() first.
        The output format is always 'YYYY-MM-DD HH:MM:SS' without timezone suffix.
    """
    # If input is a string, parse it first
    if isinstance(dt, str):
        dt = parse_date(dt)
    
    # Convert to Pacific timezone
    pacific_tz = ZoneInfo("US/Pacific")
    pacific_dt = dt.astimezone(pacific_tz)
    
    # Return formatted string without timezone suffix
    return pacific_dt.strftime('%Y-%m-%d %H:%M:%S')


def convert_epoch_to_pacific(epoch_timestamp: Union[int, float]) -> str:
    """
    Convert Unix epoch timestamp to Pacific Time formatted string.

    Takes a Unix timestamp (seconds since epoch) and converts it to a
    formatted datetime string in US/Pacific timezone.

    Args:
        epoch_timestamp (Union[int, float]): Unix timestamp in seconds

    Returns:
        str: Formatted datetime string in Pacific Time (YYYY-MM-DD HH:MM:SS)

    Example:
        pacific_str = convert_epoch_to_pacific(1703001000)
        print(pacific_str)  # '2023-12-19 08:36:40'

    Note:
        The output format is always 'YYYY-MM-DD HH:MM:SS' without timezone suffix.
        Input timestamp is assumed to be in UTC (standard Unix timestamp).
    """
    # Create datetime object from epoch timestamp (UTC)
    utc_dt = datetime.fromtimestamp(epoch_timestamp, tz=timezone.utc)
    
    # Convert to Pacific timezone
    pacific_tz = ZoneInfo("US/Pacific")
    pacific_dt = utc_dt.astimezone(pacific_tz)
    
    # Return formatted string without timezone suffix
    return pacific_dt.strftime('%Y-%m-%d %H:%M:%S')


def get_current_pacific_time() -> datetime:
    """
    Get the current time in Pacific timezone.

    Returns:
        datetime: Current datetime in US/Pacific timezone

    Example:
        now_pacific = get_current_pacific_time()
        print(now_pacific)  # 2024-12-18 06:30:00-08:00
    """
    pacific_tz = ZoneInfo("US/Pacific")
    return datetime.now(pacific_tz)


def format_time_for_db(dt: datetime) -> str:
    """
    Format a datetime object for database storage.

    Removes microseconds and timezone info to create a clean datetime string
    suitable for SQL Server DATETIME2(0) columns.

    Args:
        dt (datetime): Datetime object to format

    Returns:
        str: Formatted datetime string (YYYY-MM-DD HH:MM:SS)

    Example:
        from datetime import datetime
        dt = datetime.now()
        db_str = format_time_for_db(dt)
        print(db_str)  # '2024-12-18 14:30:45'
    """
    # Remove microseconds and timezone info for clean database storage
    clean_dt = dt.replace(microsecond=0, tzinfo=None)
    return clean_dt.strftime('%Y-%m-%d %H:%M:%S')


def get_dte_date(dte: int):
    """
    Calculate a date based on Days To Expiration (DTE).

    Calculates a future date by adding the specified number of days to today's date.
    This is commonly used for options chains to specify expiration date ranges.

    Args:
        dte (int): Days to expiration from today (0 = today, 1 = tomorrow, etc.)

    Returns:
        date: Date object representing the target date

    Example:
        # Get today's date
        today = get_dte_date(0)
        print(today)  # 2024-12-18

        # Get date 7 days from now
        week_out = get_dte_date(7)
        print(week_out)  # 2024-12-25

    Note:
        This function is primarily used for Schwab API option chains requests
        where date ranges are specified using days to expiration.
    """
    from datetime import date, timedelta

    return (date.today() + timedelta(days=dte))
