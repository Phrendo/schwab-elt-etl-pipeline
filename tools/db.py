"""
Database Connection Module

This module provides a comprehensive database interface for the Schwab API project.
It handles SQL Server connections using SQLAlchemy with support for both
transient and persistent database sessions.

Features:
- SQL Server connection via SQLAlchemy ORM
- Support for both persistent and transient sessions
- Parameterized query execution for security
- Automatic connection management and cleanup
- Environment-based configuration

Dependencies:
- sqlalchemy: For database ORM and connection management
- pyodbc: For SQL Server connectivity (via ODBC Driver 17)
- tools.config: For centralized configuration management

Security:
- Uses parameterized queries to prevent SQL injection
- Credentials managed via environment variables
- Automatic session cleanup for transient connections
"""

import os
import time
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import URL
from tools.config import get_config


class DB:
    """
    Database connection and session management class.

    Provides a high-level interface for executing SQL queries against a SQL Server
    database. Supports both one-off queries (transient sessions) and long-running
    operations (persistent sessions).

    Attributes:
        connection_url (URL): SQLAlchemy connection URL
        engine (Engine): SQLAlchemy database engine
        Session (sessionmaker): Session factory for creating database sessions
        _persistent_connection (Session): Cached persistent session instance

    Environment Variables Required:
        SQL_USERNAME: Database username
        SQL_PASSWORD: Database password
        SQL_HOST: Database server hostname
        SQL_PORT: Database server port (default: 1433)
        SQL_DATABASE: Database name (default: 'OPT')
        SQL_DRIVER: ODBC driver name (default: 'ODBC Driver 17 for SQL Server')
    """
    def __init__(self):
        """
        Initialize the database connection.

        Loads configuration from centralized config system and creates the SQLAlchemy
        engine and session factory. Validates that required credentials are present.

        Raises:
            ValueError: If required environment variables are missing
            Exception: If database connection cannot be established
        """
        # Load database configuration from centralized config system
        self._load_config()

        # Create SQLAlchemy connection URL
        self.connection_url = URL.create(
            "mssql+pyodbc",
            username=self.config['username'],
            password=self.config['password'],
            host=self.config['host'],
            port=self.config['port'],
            database=self.config['database'],
            query={
                "driver": self.config['driver'],
                "autocommit": "True",  # Enable autocommit for non-transactional operations
                "TrustServerCertificate": "yes",  # Trust server certificate for SSL
            },
        )

        # Create database engine and session factory
        self.engine = create_engine(self.connection_url)
        self.Session = sessionmaker(bind=self.engine)
        self._persistent_connection = None

        # Load market hours configuration for get_next_session method
        self._load_market_hours_config()

    def _load_config(self):
        """
        Load and validate database configuration from centralized config system.

        Uses the centralized configuration manager to load database settings,
        combining secrets from .env with configuration from config.yaml.

        Raises:
            ValueError: If required database credentials are missing
        """
        # Get configuration from centralized config system
        config = get_config()
        self.config = config.get_database_config()

    def get_session(self, persistent=False):
        """
        Get a database session for executing queries.

        Returns either a new transient session or reuses an existing persistent
        session based on the persistent parameter.

        Args:
            persistent (bool): If True, returns a cached persistent session.
                             If False, returns a new transient session.

        Returns:
            Session: SQLAlchemy session object for database operations

        Note:
            Persistent sessions must be manually closed using close_persistent_connection().
            Transient sessions are automatically closed after query execution.
        """
        if persistent:
            # Return cached persistent session, create if doesn't exist
            if not self._persistent_connection:
                self._persistent_connection = self.Session()
            return self._persistent_connection
        else:
            # Return new transient session
            return self.Session()

    def execute_query(self, query, params=None, persistent=False):
        """
        Execute a SELECT query and return results.

        Executes a parameterized SQL query and returns all matching rows.
        Automatically handles session management based on persistence setting.

        Args:
            query (str): SQL query string (can contain named parameters like :param_name)
            params (dict, optional): Dictionary of parameter values for the query
            persistent (bool): Whether to use a persistent session (default: False)

        Returns:
            list: List of Row objects containing query results

        Example:
            results = db.execute_query(
                "SELECT * FROM users WHERE name = :name",
                {"name": "John"}
            )

        Security:
            Uses parameterized queries to prevent SQL injection attacks.
        """
        session = self.get_session(persistent)
        try:
            # Execute parameterized query for security
            result = session.execute(text(query), params).fetchall()
            return result
        finally:
            # Clean up transient sessions automatically
            if not persistent:
                session.close()

    def execute_non_query(self, query, params=None, persistent=False):
        """
        Execute an INSERT, UPDATE, or DELETE query.

        Executes a parameterized SQL query that modifies data and commits
        the transaction. Automatically handles session management.

        Args:
            query (str): SQL query string (can contain named parameters like :param_name)
            params (dict, optional): Dictionary of parameter values for the query
            persistent (bool): Whether to use a persistent session (default: False)

        Example:
            db.execute_non_query(
                "UPDATE users SET email = :email WHERE id = :user_id",
                {"email": "new@example.com", "user_id": 123}
            )

        Security:
            Uses parameterized queries to prevent SQL injection attacks.

        Note:
            Automatically commits the transaction after execution.
        """
        session = self.get_session(persistent)
        try:
            # Execute parameterized query for security
            session.execute(text(query), params)
            session.commit()  # Commit the transaction
        finally:
            # Clean up transient sessions automatically
            if not persistent:
                session.close()

    def insert_balances(self, balance_data: dict):
        """
        Insert account balance data into the database.

        Inserts balance information including account details, trading restrictions,
        and financial balances into the appropriate database table. This method
        is designed to work with data from the Schwab account balance API.

        Args:
            balance_data (dict): Dictionary containing balance information with keys:
                - ApiCallTime (datetime): Timestamp when the API call was made
                - accountId (int): Account number/identifier
                - roundTrips (int): Number of round trips for day trading
                - isDayTrader (int): Day trader status (0 or 1)
                - isClosingOnly (int): Closing only restriction status (0 or 1)
                - buyingPower (float): Day trading buying power
                - cashBalance (float): Cash balance in the account
                - liquidationValue (float): Total liquidation value

        Raises:
            Exception: If there is an issue connecting to or inserting into the database.

        Example:
            db = DB()
            balance_data = {
                "ApiCallTime": datetime.now(),
                "accountId": 12345678,
                "roundTrips": 0,
                "isDayTrader": 0,
                "isClosingOnly": 0,
                "buyingPower": 25000.00,
                "cashBalance": 10000.00,
                "liquidationValue": 35000.00
            }
            db.insert_balances(balance_data)

        Security:
            Uses parameterized queries to prevent SQL injection attacks.

        Note:
            This method uses a transient session that is automatically closed
            after the insert operation. The table structure is assumed to exist
            in the OPT.SCHWAB schema.
        """
        # Define the INSERT query for balance data
        # Assumes table exists in OPT.SCHWAB schema with appropriate columns
        query = """
            INSERT INTO OPT.SCHWAB.BALANCES (
                ApiCallTime, accountId, roundTrips, isDayTrader, isClosingOnly,
                buyingPower, cashBalance, liquidationValue
            ) VALUES (
                :ApiCallTime, :accountId, :roundTrips, :isDayTrader, :isClosingOnly,
                :buyingPower, :cashBalance, :liquidationValue
            )
        """

        try:
            # Execute the insert using parameterized query for security
            self.execute_non_query(query, balance_data)
        except Exception as e:
            # Re-raise with more context for debugging
            raise Exception(f"Database error while inserting balance data: {str(e)}") from e

    def insert_account_hash(self, hash_data: dict):
        """
        Insert or update account hash data in the SCHWAB.HASH table.

        Stores account hash information that is required for account-specific
        Schwab API operations. This method handles both new inserts and updates
        of existing records for the same API name.

        Args:
            hash_data (dict): Dictionary containing hash information with keys:
                - name (str): API instance name identifier
                - account_number (str): 8-digit account number
                - account_hash (str): 64-character encrypted hash from Schwab
                - update_time (datetime): Timestamp when the hash was obtained

        Raises:
            Exception: If there is an issue connecting to or inserting into the database.

        Example:
            db = DB()
            hash_data = {
                "name": "MAIN_TRADE",
                "account_number": "12345678",
                "account_hash": "abc123def456...",
                "update_time": datetime.now()
            }
            db.insert_account_hash(hash_data)

        Security:
            Uses parameterized queries to prevent SQL injection attacks.

        Note:
            This method uses MERGE to handle both insert and update scenarios.
            If a record with the same name already exists, it updates the existing
            record with new hash and timestamp information.
        """
        # Use MERGE to handle both insert and update scenarios
        query = """
            MERGE SCHWAB.HASH AS target
            USING (VALUES (:name, :account_number, :account_hash, :update_time)) AS source (name, account_number, account_hash, update_time)
            ON target.Name = source.name
            WHEN MATCHED THEN
                UPDATE SET
                    account_number = source.account_number,
                    account_hash = source.account_hash,
                    update_time = source.update_time
            WHEN NOT MATCHED THEN
                INSERT (Name, account_number, account_hash, update_time)
                VALUES (source.name, source.account_number, source.account_hash, source.update_time);
        """

        try:
            # Execute the merge using parameterized query for security
            self.execute_non_query(query, hash_data)
        except Exception as e:
            # Re-raise with more context for debugging
            raise Exception(f"Database error while inserting/updating account hash data: {str(e)}") from e

    def close_persistent_connection(self):
        """
        Close the persistent database connection.

        Closes and clears the cached persistent session. Should be called
        when finished with long-running database operations to free resources.

        Note:
            This only affects persistent sessions. Transient sessions are
            automatically closed after each query execution.
        """
        if self._persistent_connection:
            self._persistent_connection.close()
            self._persistent_connection = None

    def _load_market_hours_config(self):
        """
        Load market hours configuration from centralized config system.

        Loads schema and table names for market hours data from the
        centralized configuration system.
        """
        config = get_config()
        market_hours_config = config.get('database.market_hours', {})
        self.market_hours_config = {
            'schema': market_hours_config.get('schema', 'OPT.SCHWAB'),
            'table': market_hours_config.get('table', 'MARKET_HOURS')
        }

    def get_token(self, api_name: str) -> str:
        """
        Fetch the access token string from the OPT.SCHWAB.API table for the given api_name.

        Retrieves the current access token for a specified API configuration from the
        database. This method is used to obtain valid authentication tokens for
        Schwab API requests.

        Args:
            api_name (str): The 'name' column in OPT.SCHWAB.API identifying which token to fetch.
                          Common values include API names for data and trading endpoints.

        Returns:
            str: The access_token retrieved from the database.

        Raises:
            ValueError: If no row is found matching the provided api_name.
            Exception: If there is an issue connecting to or querying the database.

        Example:
            db = DB()
            token = db.get_token('DATA_API')
            # Use token for API authentication

        Security:
            Uses parameterized queries to prevent SQL injection attacks.

        Note:
            This method uses a transient session that is automatically closed
            after the query execution.
        """
        query = "SELECT access_token FROM OPT.SCHWAB.API WHERE name = :api_name"
        params = {"api_name": api_name}

        try:
            result = self.execute_query(query, params)
            if not result:
                raise ValueError(f"No token found for API name '{api_name}'")
            return result[0].access_token
        except Exception as e:
            # Re-raise with more context if it's not already a ValueError
            if not isinstance(e, ValueError):
                raise Exception(f"Database error while fetching token for '{api_name}': {str(e)}") from e
            raise

    def get_next_session(self) -> tuple[float, float] | tuple[None, None]:
        """
        Retrieve the next open market session (start and end Unix timestamps) from the MARKET_HOURS table.

        Queries the market hours database to find the next trading session that is
        currently open or will open in the future. This is useful for determining
        when the market will be available for trading operations.

        Logic:
            - Selects the next row where is_open = 1 AND
              (market_date > today OR (market_date = today AND session_end > current_time))
            - Returns the earliest matching session with session_start and session_end times
            - Converts database datetime values to Unix timestamps for easy comparison

        Returns:
            tuple[float, float] | tuple[None, None]: A tuple containing:
                - (start_epoch, end_epoch): Unix timestamps in seconds for session start and end
                - (None, None): If no future session is found or on parse failure

        Raises:
            Exception: If there is an issue connecting to or querying the database.

        Example:
            db = DB()
            start_time, end_time = db.get_next_session()
            if start_time and end_time:
                print(f"Next session: {start_time} to {end_time}")
                # Check if market is currently open
                current_time = time.time()
                is_open = start_time <= current_time <= end_time
            else:
                print("No upcoming market sessions found")

        Security:
            Uses parameterized queries to prevent SQL injection attacks.

        Note:
            This method uses a transient session that is automatically closed
            after the query execution. Market hours data should be maintained
            in the configured schema and table (default: OPT.SCHWAB.MARKET_HOURS).
        """
        # Get current date and time strings for comparison
        today_str = time.strftime("%Y-%m-%d")
        now_time = time.strftime("%H:%M:%S")

        # Build the SQL query using configured schema and table names
        schema = self.market_hours_config['schema']
        table = self.market_hours_config['table']

        query = f"""
            SELECT TOP 1 market_date, session_start, session_end
            FROM {schema}.{table}
            WHERE is_open = 1
              AND ((market_date > :today_str) OR (market_date = :today_str AND session_end > :now_time))
            ORDER BY market_date ASC, session_start ASC
        """

        params = {
            "today_str": today_str,
            "now_time": now_time
        }

        try:
            result = self.execute_query(query, params)
            if not result:
                return None, None

            # Unpack the returned row: market_date is a datetime.date object
            row = result[0]
            next_date, sess_start, sess_end = row.market_date, row.session_start, row.session_end

            # Format them back into full datetime strings
            date_str = next_date.strftime("%Y-%m-%d")
            dt_start = f"{date_str} {sess_start}"
            dt_end = f"{date_str} {sess_end}"

            try:
                # Convert "YYYY-MM-DD HH:MM:SS" into Unix timestamps (seconds since epoch)
                start_epoch = time.mktime(time.strptime(dt_start, "%Y-%m-%d %H:%M:%S"))
                end_epoch = time.mktime(time.strptime(dt_end, "%Y-%m-%d %H:%M:%S"))
                return start_epoch, end_epoch
            except ValueError:
                # If parsing fails (malformed data), return no session
                return None, None

        except Exception as e:
            raise Exception(f"Database error while fetching next market session: {str(e)}") from e

    def load_market_schedule(self, days_ahead: int = 30) -> dict:
        """
        Load market hours from database for current and future dates.

        Retrieves market schedule data for a specified number of days ahead,
        starting from today. This is used by the stream controller to determine
        trading days and market hours.

        Args:
            days_ahead (int): Number of days ahead to load schedule for (default: 30)

        Returns:
            dict: Dictionary with date objects as keys and schedule info as values.
                 Each value contains: {'start': time, 'end': time, 'is_open': bool}

        Raises:
            Exception: If there is an issue connecting to or querying the database.

        Example:
            db = DB()
            schedule = db.load_market_schedule(30)
            for date, info in schedule.items():
                if info['is_open']:
                    print(f"Market open on {date}: {info['start']} - {info['end']}")

        Security:
            Uses parameterized queries to prevent SQL injection attacks.

        Note:
            This method uses a transient session that is automatically closed
            after the query execution. Returns empty dict if no data found.
        """
        from datetime import date

        # Build the SQL query using configured schema and table names
        schema = self.market_hours_config['schema']
        table = self.market_hours_config['table']

        query = f"""
            SELECT market_date, session_start, session_end, is_open
            FROM {schema}.{table}
            WHERE market_date >= :today
            ORDER BY market_date
        """

        params = {"today": date.today()}

        try:
            result = self.execute_query(query, params)

            market_schedule = {}
            for row in result:
                market_date, session_start, session_end, is_open = row
                market_schedule[market_date] = {
                    'start': session_start,
                    'end': session_end,
                    'is_open': bool(is_open)
                }

            return market_schedule

        except Exception as e:
            raise Exception(f"Database error while loading market schedule: {str(e)}") from e

    def upsert_market_hours(self, market_hours_data: dict):
        """
        Insert or update market hours data in the MARKET_HOURS table.

        Stores market hours information including session times, market type,
        and open/closed status. This method handles both new inserts and updates
        of existing records for the same market date.

        Args:
            market_hours_data (dict): Dictionary containing market hours information with keys:
                - ProcTime (datetime): Timestamp when the data was processed
                - market_date (date): Market date for the hours data
                - market_type (str): Type of market (e.g., 'REGULAR', 'HOLIDAY')
                - session_start (str): Market session start time (HH:MM:SS format)
                - session_end (str): Market session end time (HH:MM:SS format)
                - is_open (int): Market open status (1 for open, 0 for closed)

        Raises:
            Exception: If there is an issue connecting to or inserting into the database.

        Example:
            db = DB()
            from datetime import datetime, date
            market_data = {
                "ProcTime": datetime.now(),
                "market_date": date.today(),
                "market_type": "REGULAR",
                "session_start": "06:30:00",
                "session_end": "13:00:00",
                "is_open": 1
            }
            db.upsert_market_hours(market_data)

        Security:
            Uses parameterized queries to prevent SQL injection attacks.

        Note:
            This method uses MERGE to handle both insert and update scenarios.
            If a record with the same market_date already exists, it updates the
            existing record with new market hours information.
        """
        # Build the SQL query using configured schema and table names
        schema = self.market_hours_config['schema']
        table = self.market_hours_config['table']

        # Use MERGE to handle both insert and update scenarios
        query = f"""
            MERGE INTO {schema}.{table} AS target
            USING (SELECT :market_date AS market_date) AS source
            ON target.market_date = source.market_date
            WHEN MATCHED THEN
                UPDATE SET
                    ProcTime = :ProcTime,
                    market_type = :market_type,
                    session_start = :session_start,
                    session_end = :session_end,
                    is_open = :is_open
            WHEN NOT MATCHED THEN
                INSERT (ProcTime, market_date, market_type, session_start, session_end, is_open)
                VALUES (:ProcTime, :market_date, :market_type, :session_start, :session_end, :is_open);
        """

        try:
            # Execute the merge using parameterized query for security
            self.execute_non_query(query, market_hours_data)
        except Exception as e:
            # Re-raise with more context for debugging
            raise Exception(f"Database error while upserting market hours data: {str(e)}") from e

    def execute_stored_procedure(self, procedure_name: str, params: dict = None):
        """
        Execute a stored procedure in the database.

        Executes a stored procedure with optional parameters and handles
        the database session management automatically.

        Args:
            procedure_name (str): Name of the stored procedure to execute
                                (e.g., 'PYTHON.SP_PY_PROCESS_OHLC')
            params (dict, optional): Dictionary of parameter values for the procedure

        Raises:
            Exception: If there is an issue connecting to or executing the procedure.

        Example:
            db = DB()
            # Execute procedure without parameters
            db.execute_stored_procedure('PYTHON.SP_PY_PROCESS_OHLC')

            # Execute procedure with parameters
            db.execute_stored_procedure('PYTHON.SP_PROCESS_DATA', {'param1': 'value1'})

        Security:
            Uses parameterized execution to prevent SQL injection attacks.

        Note:
            This method uses a transient session that is automatically closed
            after the procedure execution.
        """
        # Build the EXEC statement
        if params:
            # Create parameter placeholders
            param_placeholders = ', '.join([f":{key}" for key in params.keys()])
            query = f"EXEC {procedure_name} {param_placeholders}"
        else:
            query = f"EXEC {procedure_name}"

        try:
            # Execute the stored procedure using parameterized query for security
            self.execute_non_query(query, params)
        except Exception as e:
            # Re-raise with more context for debugging
            raise Exception(f"Database error while executing stored procedure '{procedure_name}': {str(e)}") from e

    def df_to_sql(self, df, table_name: str, if_exists: str = 'append', schema_name: str = None):
        """
        Write DataFrame to SQL database table.

        Writes a pandas DataFrame to a SQL Server table using SQLAlchemy's
        to_sql method with proper connection management.

        Args:
            df (pandas.DataFrame): DataFrame to write to database
            table_name (str): Name of the target table
            if_exists (str): How to behave if the table exists:
                           - 'fail': Raise a ValueError
                           - 'replace': Drop the table before inserting new values
                           - 'append': Insert new values to the existing table (default)
            schema_name (str, optional): Schema name for the table (e.g., 'PYTHON')

        Raises:
            Exception: If there is an issue connecting to or writing to the database.

        Example:
            db = DB()
            import pandas as pd
            df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
            db.df_to_sql(df, 'test_table', if_exists='append', schema_name='PYTHON')

        Note:
            This method uses the SQLAlchemy engine directly for pandas integration.
            The DataFrame is written in a single transaction.
        """
        try:
            # Use pandas to_sql with SQLAlchemy engine
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists=if_exists,
                index=False,  # Don't write DataFrame index as a column
                schema=schema_name,
                method='multi'  # Use multi-row INSERT for better performance
            )
        except Exception as e:
            # Re-raise with more context for debugging
            table_full_name = f"{schema_name}.{table_name}" if schema_name else table_name
            raise Exception(f"Database error while writing DataFrame to table '{table_full_name}': {str(e)}") from e

    def insert_raw_json(self, order: dict):
        """
        Insert raw JSON order data into OPT.SCHWAB.JSON_TRANSACTIONS table.

        Stores the complete order JSON data along with key metadata for tracking
        and processing purposes. This method handles duplicate prevention.

        Args:
            order (dict): Order dictionary from Schwab API containing order details

        Raises:
            Exception: If there is an issue connecting to or inserting into the database.

        Example:
            db = DB()
            order_data = {
                "orderId": "12345",
                "status": "FILLED",
                "enteredTime": "2024-12-18T09:30:00.000Z",
                # ... other order fields
            }
            db.insert_raw_json(order_data)

        Security:
            Uses parameterized queries to prevent SQL injection attacks.

        Note:
            This method uses IF NOT EXISTS to prevent duplicate entries based on
            OrderID, Status, and enteredTime combination.
        """
        import json
        from tools.utils import parse_date, convert_to_pacific_time

        # Extract key fields and convert JSON to string
        json_data = json.dumps(order)
        order_id = order["orderId"]
        status = order["status"]
        entered_time = parse_date(order["enteredTime"])
        entered_time_pacific = convert_to_pacific_time(entered_time)

        # SQL query with duplicate prevention
        query = """
            IF NOT EXISTS (
                SELECT 1 FROM OPT.SCHWAB.JSON_TRANSACTIONS
                WHERE OrderID = :orderId AND Status = :status AND enteredTime = :enteredTime
            )
            BEGIN
                INSERT INTO OPT.SCHWAB.JSON_TRANSACTIONS (OrderID, Status, enteredTime, JsonData)
                VALUES (:orderId, :status, :enteredTime, :jsonData)
            END
        """

        params = {
            "orderId": order_id,
            "status": status,
            "enteredTime": entered_time_pacific,
            "jsonData": json_data,
        }

        try:
            # Execute the insert using parameterized query for security
            self.execute_non_query(query, params)
        except Exception as e:
            # Re-raise with more context for debugging
            raise Exception(f"Database error while inserting raw JSON for order '{order_id}': {str(e)}") from e

    def insert_order(self, order: dict, parent_order_id: str = None):
        """
        Insert order details into PYTHON.Orders table.

        Stores structured order information with duplicate prevention based on orderId.
        Handles timezone conversion for time fields.

        Args:
            order (dict): Order dictionary from Schwab API
            parent_order_id (str, optional): Parent order ID for child orders

        Raises:
            Exception: If there is an issue connecting to or inserting into the database.

        Security:
            Uses parameterized queries to prevent SQL injection attacks.

        Note:
            This method uses IF NOT EXISTS to prevent duplicate entries based on orderId.
        """
        from tools.utils import parse_date, convert_to_pacific_time

        # Parse and convert time fields
        entered_time = parse_date(order['enteredTime'])
        close_time = parse_date(order['closeTime']) if 'closeTime' in order else None

        entered_time_pacific = convert_to_pacific_time(entered_time) if entered_time else None
        close_time_pacific = convert_to_pacific_time(close_time) if close_time else None

        # SQL query with duplicate prevention
        query = """
            IF NOT EXISTS (
                SELECT 1 FROM PYTHON.Orders
                WHERE orderId = :orderId
            )
            BEGIN
                INSERT INTO PYTHON.Orders (orderId, session, duration, orderType, complexOrderStrategyType, quantity, filledQuantity,
                                    remainingQuantity, requestedDestination, destinationLinkName, stopPrice, stopType,
                                    orderStrategyType, cancelable, editable, status, enteredTime, closeTime, tag, accountNumber, parentOrderId)
                VALUES (:orderId, :session, :duration, :orderType, :complexOrderStrategyType, :quantity, :filledQuantity,
                        :remainingQuantity, :requestedDestination, :destinationLinkName, :stopPrice, :stopType,
                        :orderStrategyType, :cancelable, :editable, :status, :enteredTime, :closeTime, :tag, :accountNumber, :parentOrderId)
            END
        """

        params = {
            "orderId": order['orderId'],
            "session": order.get('session'),
            "duration": order.get('duration'),
            "orderType": order.get('orderType'),
            "complexOrderStrategyType": order.get('complexOrderStrategyType'),
            "quantity": order.get('quantity'),
            "filledQuantity": order.get('filledQuantity'),
            "remainingQuantity": order.get('remainingQuantity'),
            "requestedDestination": order.get('requestedDestination'),
            "destinationLinkName": order.get('destinationLinkName'),
            "stopPrice": order.get('stopPrice'),
            "stopType": order.get('stopType'),
            "orderStrategyType": order.get('orderStrategyType'),
            "cancelable": order.get('cancelable'),
            "editable": order.get('editable'),
            "status": order.get('status'),
            "enteredTime": entered_time_pacific,
            "closeTime": close_time_pacific,
            "tag": order.get('tag'),
            "accountNumber": order.get('accountNumber'),
            "parentOrderId": parent_order_id,
        }

        try:
            # Execute the insert using parameterized query for security
            self.execute_non_query(query, params)
        except Exception as e:
            # Re-raise with more context for debugging
            raise Exception(f"Database error while inserting order '{order['orderId']}': {str(e)}") from e

    def insert_order_leg(self, order_id: str, leg: dict):
        """
        Insert order legs into PYTHON.OrderLegs table.

        Stores individual order leg information with instrument details.
        Handles duplicate prevention based on legId and orderId combination.

        Args:
            order_id (str): The order ID this leg belongs to
            leg (dict): Order leg dictionary from Schwab API

        Raises:
            Exception: If there is an issue connecting to or inserting into the database.

        Security:
            Uses parameterized queries to prevent SQL injection attacks.

        Note:
            This method uses IF NOT EXISTS to prevent duplicate entries.
        """
        instrument = leg['instrument']

        # SQL query with duplicate prevention
        query = """
            IF NOT EXISTS (
                SELECT 1 FROM PYTHON.OrderLegs
                WHERE legId = :legId AND orderId = :orderId
            )
            BEGIN
                INSERT INTO PYTHON.OrderLegs (legId, orderId, orderLegType, assetType, cusip, symbol, description, instrumentId, type,
                                    putCall, underlyingSymbol, instruction, positionEffect, quantity)
                VALUES (:legId, :orderId, :orderLegType, :assetType, :cusip, :symbol, :description, :instrumentId, :type,
                        :putCall, :underlyingSymbol, :instruction, :positionEffect, :quantity)
            END
        """

        params = {
            "legId": leg['legId'],
            "orderId": order_id,
            "orderLegType": leg.get('orderLegType'),
            "assetType": instrument.get('assetType'),
            "cusip": instrument.get('cusip'),
            "symbol": instrument.get('symbol'),
            "description": instrument.get('description'),
            "instrumentId": instrument.get('instrumentId'),
            "type": instrument.get('type'),
            "putCall": instrument.get('putCall'),
            "underlyingSymbol": instrument.get('underlyingSymbol'),
            "instruction": leg.get('instruction'),
            "positionEffect": leg.get('positionEffect'),
            "quantity": leg.get('quantity'),
        }

        try:
            # Execute the insert using parameterized query for security
            self.execute_non_query(query, params)
        except Exception as e:
            # Re-raise with more context for debugging
            raise Exception(f"Database error while inserting order leg for order '{order_id}': {str(e)}") from e

    def insert_order_activity(self, order_id: str, activity: dict) -> int:
        """
        Insert order activity into PYTHON.OrderActivities table.

        Stores order activity information and returns the activity ID for
        linking execution legs.

        Args:
            order_id (str): The order ID this activity belongs to
            activity (dict): Order activity dictionary from Schwab API

        Returns:
            int: The activity ID of the inserted record, or None if insertion failed

        Raises:
            Exception: If there is an issue connecting to or inserting into the database.

        Security:
            Uses parameterized queries to prevent SQL injection attacks.

        Note:
            This method uses IF NOT EXISTS to prevent duplicate entries and
            retrieves the activity ID for linking execution legs.
        """
        # SQL query with duplicate prevention
        query = """
            IF NOT EXISTS (
                SELECT 1 FROM PYTHON.OrderActivities
                WHERE orderId = :orderId AND activityType = :activityType AND executionType = :executionType AND quantity = :quantity AND orderRemainingQuantity = :orderRemainingQuantity
            )
            BEGIN
                INSERT INTO PYTHON.OrderActivities (orderId, activityType, executionType, quantity, orderRemainingQuantity)
                VALUES (:orderId, :activityType, :executionType, :quantity, :orderRemainingQuantity)
            END
        """

        params = {
            "orderId": order_id,
            "activityType": activity.get('activityType'),
            "executionType": activity.get('executionType'),
            "quantity": activity.get('quantity'),
            "orderRemainingQuantity": activity.get('orderRemainingQuantity'),
        }

        try:
            # Execute the insert using parameterized query for security
            self.execute_non_query(query, params)

            # Retrieve the inserted activity_id
            activity_id_query = """
                SELECT TOP 1 activityId FROM PYTHON.OrderActivities
                WHERE orderId = :orderId AND activityType = :activityType
                ORDER BY activityId DESC
            """

            result = self.execute_query(activity_id_query, {
                "orderId": order_id,
                "activityType": activity.get('activityType'),
            })

            return result[0].activityId if result else None

        except Exception as e:
            # Re-raise with more context for debugging
            raise Exception(f"Database error while inserting order activity for order '{order_id}': {str(e)}") from e

    def insert_execution_leg(self, activity_id: int, leg: dict):
        """
        Insert execution legs into PYTHON.ExecutionLegs table.

        Stores execution leg information linked to an order activity.
        Handles duplicate prevention and time conversion.

        Args:
            activity_id (int): The activity ID this execution leg belongs to
            leg (dict): Execution leg dictionary from Schwab API

        Raises:
            Exception: If there is an issue connecting to or inserting into the database.

        Security:
            Uses parameterized queries to prevent SQL injection attacks.

        Note:
            This method uses IF NOT EXISTS to prevent duplicate entries and
            converts execution time to proper format.
        """
        from tools.utils import parse_date

        # Parse execution time
        execution_time = parse_date(leg['time'])

        # SQL query with duplicate prevention
        query = """
            IF NOT EXISTS (
                SELECT 1 FROM PYTHON.ExecutionLegs
                WHERE activityId = :activityId AND legId = :legId
            )
            BEGIN
                INSERT INTO PYTHON.ExecutionLegs (activityId, legId, quantity, mismarkedQuantity, price, time, instrumentId)
                VALUES (:activityId, :legId, :quantity, :mismarkedQuantity, :price, :time, :instrumentId)
            END
        """

        params = {
            "activityId": activity_id,
            "legId": leg['legId'],
            "quantity": leg['quantity'],
            "mismarkedQuantity": leg['mismarkedQuantity'],
            "price": leg['price'],
            "time": execution_time,
            "instrumentId": leg['instrumentId'],
        }

        try:
            # Execute the insert using parameterized query for security
            self.execute_non_query(query, params)
        except Exception as e:
            # Re-raise with more context for debugging
            raise Exception(f"Database error while inserting execution leg for activity '{activity_id}': {str(e)}") from e

    def process_order(self, order: dict, parent_order_id: str = None):
        """
        Process the full order structure, including legs, activities, and execution legs.

        This is the main method for processing complete order data from Schwab API.
        It handles the entire order hierarchy including child orders recursively.

        Args:
            order (dict): Complete order dictionary from Schwab API
            parent_order_id (str, optional): Parent order ID for child orders

        Raises:
            Exception: If there is an issue processing any part of the order structure.

        Example:
            db = DB()
            order_data = {
                "orderId": "12345",
                "status": "FILLED",
                "orderLegCollection": [...],
                "orderActivityCollection": [...],
                "childOrderStrategies": [...]
            }
            db.process_order(order_data)

        Note:
            This method processes orders recursively to handle child order strategies.
            It maintains referential integrity between orders, legs, activities, and executions.
        """
        try:
            # Insert the main order
            self.insert_order(order, parent_order_id)
            order_id = order['orderId']

            # Process order legs
            if 'orderLegCollection' in order:
                for leg in order['orderLegCollection']:
                    self.insert_order_leg(order_id, leg)

            # Process order activities and their execution legs
            if 'orderActivityCollection' in order:
                for activity in order['orderActivityCollection']:
                    activity_id = self.insert_order_activity(order_id, activity)
                    if activity_id and 'executionLegs' in activity:
                        for leg in activity['executionLegs']:
                            self.insert_execution_leg(activity_id, leg)

            # Process child orders recursively
            if 'childOrderStrategies' in order:
                for child_order in order['childOrderStrategies']:
                    self.process_order(child_order, order_id)

        except Exception as e:
            # Re-raise with more context for debugging
            raise Exception(f"Database error while processing order '{order.get('orderId', 'UNKNOWN')}': {str(e)}") from e





