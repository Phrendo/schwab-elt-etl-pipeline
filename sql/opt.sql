USE [OPT]
GO
CREATE SCHEMA [HISTORIC]
GO
CREATE SCHEMA [PYTHON]
GO
CREATE SCHEMA [SCHWAB]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [HISTORIC].[DAY]
(
	[O] [decimal](9, 2) NULL,
	[H] [decimal](9, 2) NULL,
	[L] [decimal](9, 2) NULL,
	[C] [decimal](9, 2) NULL,
	[Dt] [date] NULL,
	[Sym] [varchar](6) NULL
) ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [HISTORIC].[MINUTE]
(
	[O] [decimal](9, 2) NULL,
	[H] [decimal](9, 2) NULL,
	[L] [decimal](9, 2) NULL,
	[C] [decimal](9, 2) NULL,
	[V] [bigint] NULL,
	[Dt] [datetime] NULL,
	[Sym] [varchar](6) NULL
) ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [PYTHON].[DAY]
(
	[open] [float] NULL,
	[high] [float] NULL,
	[low] [float] NULL,
	[close] [float] NULL,
	[volume] [bigint] NULL,
	[datetime] [datetime] NULL,
	[date] [date] NULL,
	[Symbol] [varchar](max) NULL,
	[freq] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [PYTHON].[EXECUTIONLEGS]
(
	[activityId] [int] NOT NULL,
	[legId] [nvarchar](50) NOT NULL,
	[quantity] [float] NULL,
	[mismarkedQuantity] [float] NULL,
	[price] [float] NULL,
	[time] [datetimeoffset](7) NULL,
	[instrumentId] [nvarchar](50) NULL,
	CONSTRAINT [PK_ExecutionLegs] PRIMARY KEY CLUSTERED 
(
	[activityId] ASC,
	[legId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [PYTHON].[MINUTE]
(
	[open] [float] NULL,
	[high] [float] NULL,
	[low] [float] NULL,
	[close] [float] NULL,
	[volume] [bigint] NULL,
	[datetime] [datetime] NULL,
	[Symbol] [varchar](max) NULL,
	[freq] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [PYTHON].[ORDERACTIVITIES]
(
	[activityId] [int] IDENTITY(1,1) NOT NULL,
	[orderId] [nvarchar](50) NULL,
	[activityType] [nvarchar](50) NULL,
	[executionType] [nvarchar](50) NULL,
	[quantity] [float] NULL,
	[orderRemainingQuantity] [float] NULL,
	PRIMARY KEY CLUSTERED 
(
	[activityId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [PYTHON].[ORDERLEGS]
(
	[legId] [nvarchar](50) NULL,
	[orderId] [nvarchar](50) NULL,
	[orderLegType] [nvarchar](50) NULL,
	[assetType] [nvarchar](50) NULL,
	[cusip] [nvarchar](50) NULL,
	[symbol] [nvarchar](50) NULL,
	[description] [nvarchar](255) NULL,
	[instrumentId] [nvarchar](50) NULL,
	[type] [nvarchar](50) NULL,
	[putCall] [nvarchar](50) NULL,
	[underlyingSymbol] [nvarchar](50) NULL,
	[instruction] [nvarchar](50) NULL,
	[positionEffect] [nvarchar](50) NULL,
	[quantity] [float] NULL
) ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [PYTHON].[ORDERS]
(
	[orderId] [nvarchar](50) NOT NULL,
	[session] [nvarchar](50) NULL,
	[duration] [nvarchar](50) NULL,
	[orderType] [nvarchar](50) NULL,
	[complexOrderStrategyType] [nvarchar](50) NULL,
	[quantity] [float] NULL,
	[filledQuantity] [float] NULL,
	[remainingQuantity] [float] NULL,
	[requestedDestination] [nvarchar](50) NULL,
	[destinationLinkName] [nvarchar](50) NULL,
	[stopPrice] [float] NULL,
	[stopType] [nvarchar](50) NULL,
	[orderStrategyType] [nvarchar](50) NULL,
	[cancelable] [nvarchar](50) NULL,
	[editable] [nvarchar](50) NULL,
	[status] [nvarchar](50) NULL,
	[enteredTime] [datetimeoffset](7) NULL,
	[closeTime] [datetimeoffset](7) NULL,
	[tag] [nvarchar](50) NULL,
	[accountNumber] [nvarchar](50) NULL,
	[parentOrderId] [nvarchar](50) NULL,
	CONSTRAINT [PK_Orders] PRIMARY KEY CLUSTERED 
(
	[orderId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [PYTHON].[PYTHON_LOGS]
(
	[id] [int] IDENTITY(1,1) NOT NULL,
	[timestamp] [datetime] NOT NULL,
	[level] [nvarchar](10) NOT NULL,
	[message] [nvarchar](max) NOT NULL,
	[module] [nvarchar](50) NULL,
	[filename] [nvarchar](100) NULL,
	[line] [int] NULL,
	[app_name] [nvarchar](50) NOT NULL,
	[error] [nvarchar](max) NULL,
	PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [SCHWAB].[API]
(
	[Name] [varchar](10) NOT NULL,
	[client_id] [varchar](32) NOT NULL,
	[client_secret] [varchar](16) NOT NULL,
	[redirect_uri] [varchar](17) NOT NULL,
	[refresh_token] [varchar](255) NULL,
	[access_token] [varchar](255) NULL,
	[access_token_expires_at] [datetime2](7) NULL,
	[refresh_token_expires_at] [datetime2](7) NULL
) ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [SCHWAB].[BALANCES]
(
	[ApiCallTime] [datetime2](0) NOT NULL,
	[accountId] [int] NOT NULL,
	[roundTrips] [smallint] NULL,
	[isDayTrader] [smallint] NULL,
	[isClosingOnly] [smallint] NULL,
	[buyingPower] [decimal](20, 2) NULL,
	[cashBalance] [decimal](20, 2) NULL,
	[liquidationValue] [decimal](20, 2) NULL,
	CONSTRAINT [PK_Balances] PRIMARY KEY CLUSTERED 
(
	[ApiCallTime] ASC,
	[accountId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [SCHWAB].[HASH]
(
	[Name] [varchar](10) NOT NULL,
	[account_number] [varchar](8) NOT NULL,
	[account_hash] [varchar](64) NOT NULL,
	[update_time] [datetime2](7) NULL
) ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [SCHWAB].[JSON_TRANSACTIONS]
(
	[ReceivedAt] [datetime2](0) NULL,
	[JsonData] [nvarchar](max) NULL,
	[OrderID] [bigint] NULL,
	[Status] [nvarchar](50) NULL,
	[enteredTime] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [SCHWAB].[MARKET_HOURS]
(
	[ProcTime] [datetime2](0) NOT NULL,
	[market_date] [date] NOT NULL,
	[market_type] [varchar](50) NOT NULL,
	[session_start] [time](0) NULL,
	[session_end] [time](0) NULL,
	[is_open] [bit] NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [SCHWAB].[JSON_TRANSACTIONS] ADD  DEFAULT (getdate()) FOR [ReceivedAt]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [PYTHON].[SP_PY_PARSE_TRANSACTIONS]

AS
BEGIN

	SET NOCOUNT ON;

	DROP TABLE IF EXISTS #Subs
	SELECT orderId, AQ, SUM(P) P, MAX(SS) SS, MAX(LS) LS, PutCall, PositionEffect
	INTO #Subs
	FROM (
		SELECT A.orderId,
			SUM(C.quantity) AQ,
			LEFT(instruction,3) instruction,
			PositionEffect,
			IIF(LEFT(instruction,3)='BUY',NULL, Symbol) SS,
			IIF(LEFT(instruction,3)='BUY',Symbol, NULL) LS,
			PutCall,
			AVG(IIF(LEFT(instruction,3)='BUY',-C.price,C.price)) P
		FROM OPT.PYTHON.OrderActivities A
			LEFT JOIN OPT.PYTHON.OrderLegs B ON B.orderId=A.orderId
			LEFT JOIN OPT.PYTHON.ExecutionLegs C ON C.activityId=A.activityId AND C.legId=B.legId
		GROUP BY A.orderId, LEFT(instruction,3), PositionEffect, Symbol, PutCall
	) A
	GROUP BY orderId, AQ, PutCall, PositionEffect


	DELETE FROM OPT.SCHWAB.TRANSACTIONS WHERE orderId IN (SELECT orderId
	FROM OPT.PYTHON.ORDERS
	GROUP BY orderId)

	INSERT INTO OPT.SCHWAB.TRANSACTIONS
	SELECT A.orderType, A.complexOrderStrategyType V, A.orderId, A.status, A.enteredTime, A.closeTime,
		A.accountNumber, B.AQ, B.P, B.SS, B.LS, B.putCall, B.PositionEffect
	FROM OPT.PYTHON.ORDERS A
		LEFT JOIN #Subs B ON B.orderId=A.orderId
	WHERE orderStrategyType <> 'OCO' aND Status IN ('FILLED', 'AWAITING_STOP_CONDITION')

	TRUNCATE TABLE OPT.PYTHON.ORDERS
	TRUNCATE TABLE OPT.PYTHON.OrderActivities
	TRUNCATE TABLE OPT.PYTHON.OrderLegs
	TRUNCATE TABLE OPT.PYTHON.ExecutionLegs

	EXEC OPT.LIVE.HANDLE_CST


	SELECT *
	FROM OPT.PYTHON.ORDERS
	WHERE orderID = 1003548286574

END

GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		BW
-- Updated date: 02/19/2025
-- Description:	This processes the OHLC data from the staging tables.
--		This is used by SCHWAB_DATA_COLLECTOR - DAILY OHLC
-- =============================================
CREATE PROCEDURE [PYTHON].[SP_PY_PROCESS_OHLC]

AS
BEGIN
	SET NOCOUNT ON;

	---======== GATHER UNIQUE VALUES FOR DAY
	DROP TABLE IF EXISTS #T_DAY
	SELECT S.[open] O, S.[high] H, S.[low] L, S.[close] C, [date] Dt, Symbol Sym
	INTO #T_DAY
	FROM OPT.PYTHON.DAY S
	WHERE S.freq = 'DAY'
	GROUP BY S.[open], S.[high], S.[low], S.[close], [date], Symbol

	---======== INSERT WHERE NOT EXISTS
	INSERT INTO OPT.HISTORIC.DAY
		(O,H,L,C,Dt,Sym)
	SELECT S.O, S.H, S.L, S.C, S.Dt, S.Sym
	FROM #T_DAY S
		LEFT JOIN OPT.HISTORIC.DAY T ON T.dt=S.Dt AND T.Sym=S.Sym
	WHERE T.dt IS NULL

	---======== TRUNCATE THE STAGING TABLE
	TRUNCATE TABLE OPT.PYTHON.DAY

	---======== GATHER UNIQUE VALUES FOR MINUTE
	DROP TABLE IF EXISTS #T_MIN
	SELECT S.[open] O, S.[high] H, S.[low] L, S.[close] C, volume V, datetime Dt, Symbol Sym
	INTO #T_MIN
	FROM OPT.PYTHON.MINUTE S
	WHERE S.freq = 'MINUTE'
	GROUP BY S.[open], S.[high], S.[low], S.[close], volume, datetime, Symbol

	---======== INSERT WHERE NOT EXISTS
	INSERT INTO OPT.HISTORIC.MINUTE
		(O,H,L,C,V,Dt,Sym)
	SELECT S.O, S.H, S.L, S.C, S.V, S.Dt, S.Sym
	FROM #T_MIN S
		LEFT JOIN OPT.HISTORIC.MINUTE T ON T.dt=S.Dt AND T.Sym=S.Sym
	WHERE T.dt IS NULL

	---======== TRUNCATE THE STAGING TABLE
	TRUNCATE TABLE OPT.PYTHON.MINUTE

END
GO