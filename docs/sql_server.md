# SQL Server Market Data Ingestion into Data Warehouse

### Setup Information
Using a websocket, we collect live streaming data from the borkerage. That data is piped directly into parquet files. The data is also pushed directly into a **Redis cache**. This allows two purposes for the data. 

1. The data can be used for **live trading**, as it is pushed to the Redis cache.
2. The data can be used for **historical analysis**, as it is pushed to the parquet files.

## WebSocket Pipeline
```mermaid
flowchart LR
    classDef apiNode fill:#ff9800,stroke:#f57c00,color:#000
    classDef serviceNode fill:#003BA6,stroke:#007a6c,color:#fff
    classDef storageNode fill:#D82828,stroke:#0288d1,color:#000
    classDef monitorNode fill:#7e57c2,stroke:#5e35b1,color:#fff
    classDef scriptNode fill:#000,stroke:#0FAD25,color:#0FAD25
    classDef animate stroke-dasharray: 9,5,stroke-dashoffset: 50,animation: dash 2s linear infinite alternate;
    classDef animate2 stroke-dasharray: 9,5,stroke-dashoffset: 50,animation: dash 2s linear infinite;
    

    %% API
    API[("fas:fa-server Schwab API")]
    class API apiNode

    %% SQL Server
    SQL[("fas:fa-database SQL Server")]
    class SQL storageNode

    %% AUTHENTICATION
    AUTH(["fas:fa-lock Authentication"])
    class AUTH serviceNode

    %% STREAM SERVICE
    STREAM(["fas:fa-water Streaming Service"])
    class STREAM serviceNode

    %% STORAGE SYSTEMS
    REDIS[("fas:fa-memory Redis Cache")]
    PARQUET[("fas:fa-save Parquet")]
    class REDIS,PARQUET storageNode

    %% Live Trader
    TRADER(["Live Trader"])
    class TRADER scriptNode

    %% PIPELINE CONNECTIONS
    API e1@<--> AUTH e2@<--> SQL
    API e3@ <--> STREAM e4@--> REDIS
    SQL o--o STREAM
    REDIS e6@--> TRADER
    
    STREAM e5@--> PARQUET
    
    class e1,e2,e3 animate
    class e4,e5,e6 animate2
```
> Note: for detailed information on the Authentication service, please refer to the [README](../README.md).

Below is a specific example from data to vertical time series.

### Parquet Bulk Insert (Bronze)
Below you will find the initial ingestion into Bronze.  You will notice, the logging is quite simple as is the error checking. This is primarily due to the way I handle logging. I am using systemctl and journalctl (both tools build into Ubuntu), for logging and alerting and service control.  With a simply print to screen and the ETL SP executed by python, the output is sent to the journal with relevant run details from the python file.

```sql
-- =============================================
-- Author:		Bobby Whitehead
-- Create date: 06/03/2025
-- Description: Pulls in the daily parquet file on schedule,
--				allows for manually pulling in previous day parquets.
-- =============================================
ALTER PROCEDURE [SPX].[SP_IMPORT_PARQUET]
@D DATE, @RootPath NVARCHAR(100)
AS
BEGIN
	SET NOCOUNT ON;
	
	DROP TABLE SPX_OPT.SPX.PARQUET_STAGE

	DECLARE @fileDate   VARCHAR(10);
	DECLARE @filePath   NVARCHAR(255);
	DECLARE @sql        NVARCHAR(MAX);

	-- Get today’s date in yyyy-MM-dd format
	SET @fileDate = CONVERT(VARCHAR(10), @D, 23);

	-- Construct the full path to the parquet file
	SET @filePath = @RootPath + '/parquet/quotes_' + @fileDate + '.parquet';

	BEGIN TRY 
		-- Build the dynamic SQL for importing
		SET @sql = N'
			SELECT *
			INTO SPX_OPT.SPX.PARQUET_STAGE
			FROM OPENROWSET(
				BULK ''' + @filePath + ''',
				FORMAT = ''PARQUET''
			) AS rows
			ORDER BY received_at DESC;
		';

		EXEC sp_executesql @sql;
	END TRY
	BEGIN CATCH
		-- Keep message to the point, this procedure is ran from python,
		-- which outputs to journalCtl, which is where our, logging alerts live.
		PRINT('Issue with Parquet import')
	END CATCH

```
> NOTE: Removing try/catch, logging and vertically compressed scripting for readability going forward.

### Transforming into market objects (options, symbols) (Silver)
This is an example procedure that ingests from the parquet staging table. The websocket side of this (the collector) has a data cleansing layer. The data at this point is mostly pristine. This section parses data into Option Objects (OPT) and Option Time Series OPTM, in a perfect 1NF-3NF format. The only repeating data is the OPT_ID.

The final merge is more to do with multiple options data sources (several brokerages, with varying quality assignments).

```sql
-- =============================================
-- Author:		Bobby Whitehead
-- Create date: 06/03/2025
-- Description: Parses the parquet data into options and option time series data.
-- =============================================
ALTER PROCEDURE [SPX].[SP_IMPORT_PARQUET_OPT_DATA]

AS
BEGIN
	SET NOCOUNT ON;
	;WITH CTE_OPT AS (
		SELECT Strike, CP, Expiry
		FROM SPX_OPT.SPX.PARQUET_STAGE A
		CROSS APPLY dbo.ParseSPXSymbol_ITVF(A.symbol) D
		WHERE A.symbol <> '$SPX'
		GROUP BY Strike, CP, Expiry, A.symbol
	)

	-----===== INSERT UNIQUE OPTS
	INSERT INTO SPX_OPT.SPX.OPT (Strike, CP, Expiry)
	SELECT O.* FROM CTE_OPT O
	LEFT JOIN SPX_OPT.SPX.OPT B ON B.CP=O.CP AND B.Expiry=O.Expiry AND B.Strike=O.Strike
	WHERE B.Strike IS NULL AND O.Strike <> 0 AND O.CP IS NOT NULL AND O.Expiry IS NOT NULL

	-----===== CREATE THE SOURCE TABLE
	;WITH CTE_SRC AS (
		SELECT R.pst ReceivedAt, symbol, [37] Mark, T.pst T, D.*
		FROM SPX_OPT.SPX.PARQUET_STAGE A
		CROSS APPLY dbo.EpochMsToPST_ITVF(A.received_at) R
		CROSS APPLY dbo.EpochMsToPST_ITVF(A.[38]) T
		CROSS APPLY dbo.ParseSPXSymbol_ITVF(A.symbol) D
		WHERE A.symbol <> '$SPX' AND A.[37] IS NOT NULL AND A.[38] IS NOT NULL
	), CTE_OPTM AS (
		SELECT B.OPT_ID, A.T, MAX(A.Mark) Mark FROM CTE_SRC A
		INNER JOIN SPX_OPT.SPX.OPT B ON B.Expiry=A.Expiry AND B.CP=A.CP AND B.Strike=A.Strike
		WHERE CAST(A.T AS TIME(0)) BETWEEN '06:30' AND '13:00'
		GROUP BY B.OPT_ID, A.T
	)

	-----===== UPSERT SC INTO OPTM
	MERGE INTO SPX_OPT.SPX.OPTM AS Target
	USING (
		SELECT OPT_ID, T, Mark O
		FROM CTE_OPTM
	) AS Source
	  ON Target.OPT_ID = Source.OPT_ID
	 AND Target.T      = Source.T
	WHEN NOT MATCHED BY TARGET THEN
	  INSERT (OPT_ID, T, O)
	  VALUES (Source.OPT_ID, Source.T, Source.O);
END
```

### Transforming into Verticals (Gold)

```sql
-- =============================================
-- Author:		Bobby Whitehead
-- Create date: 03/29/2024
-- Description:	CREATES 0 DTE VERTS FOR A GIVEN DAY
-- =============================================
ALTER PROCEDURE [SPX].[SP_PROCESS_VERTS]
@D DATE, @MinTime TIME(0), @W INT
AS
BEGIN

	SET NOCOUNT ON;

	--DECLARE @D DATE = '2025-05-29', @MinTime TIME(0) = '10:00', @W INT = 5
	DROP TABLE IF EXISTS #SHORT_OPTS, #LONG_OPTS, #BOTH_OPTS, #BOTH_OPTS_EM, #BOTH_OPTS_EM_UL, #BOTH_OPTS_OUTLIERS_UL, #FINAL
	--Yes these are temp tables, CTE and Table variables are not as efficient for this exact transform	

	DECLARE @Expiry DATE = @D, @SPX_Min DECIMAL(9,2), @SPX_Max DECIMAL(9,2), @OPT_Range INT = 30

	------ GET THE MIN AND MAX OF SPX TO HELP BUILD THE RANGE OF STRIKES
	SELECT @SPX_Min=CAST(ROUND(MIN(Mark)/5,0)*5 AS INT), @SPX_Max=CAST(ROUND(MAX(Mark)/5,0)*5 AS INT)
	FROM SPX_OPT.SPX.UL
	WHERE CAST(T AS DATE) = @Expiry
		AND CAST(T AS TIME(0)) BETWEEN @MinTime AND DATEADD(HOUR,2,@MinTime)

	IF @SPX_Min IS NOT NULL
	BEGIN

		----- Back file spaced ticks (i.e. If an option is 2.35 at 10:05:01 and 2.40 at 10:06:01, everything in between is 2.35)
		----- We create more data, but the join is essential when building multi-leg objects (verticals, condors, flys)
		EXEC SPX_VERT.SPX.SP_OPTION_TIMESERIES_BACKFILL @Expiry, @MinTime, @SPX_Min, @SPX_Max, @OPT_Range, @W

		------ THIS MERGES THE TWO TABLES TOGETHER AND CREATES UNIQUE COLUMN NAMES
		SELECT S.Expiry, S.T, S.CP, S.OPT_ID SID, L.OPT_ID LID, S.SS, L.SS LS, S.SO, L.LO, (S.SO-L.LO) O, IIF(S.CP=-1, S.SS-L.SS, L.SS-S.SS) W INTO #BOTH_OPTS
		FROM SPX_VERT.SPX.TEMP_OPTION_TIMESERIES S
		FULL OUTER JOIN SPX_VERT.SPX.TEMP_OPTION_TIMESERIES L 
			ON L.T=S.T AND S.CP=L.CP AND ((S.SS=L.SS+@W AND S.CP=-1) OR (S.SS=L.SS-@W AND S.CP=1))
		WHERE L.T=S.T

		------ THIS LABELS THE OUTLIERS
		SELECT *,
			CASE
				WHEN ((AvgPre - A.O > .5 AND AvgFol - A.O > .5) OR (A.O - AvgPre > .5 AND A.O - AvgFol > .5)) THEN 1
			ELSE 0 END AS OI
		INTO #BOTH_OPTS_OUTLIERS_UL FROM (
			SELECT A.*,
					CAST(AVG(O) OVER(PARTITION BY SID, LID ORDER BY T ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS DECIMAL(9,2)) AVGPRE,
					CAST(AVG(O) OVER(PARTITION BY SID, LID ORDER BY T ASC ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING) AS DECIMAL(9,2)) AVGFOL
			FROM #BOTH_OPTS A) A

		INSERT INTO SPX_VERT.SPX.VERT (SID, LID, SS, W, CP, Expiry)
		SELECT A.* FROM (SELECT SID, LID, SS, W, CP, Expiry FROM #BOTH_OPTS_OUTLIERS_UL GROUP BY SID, LID, SS, W, CP, Expiry) A
		LEFT JOIN SPX_VERT.SPX.VERT B ON B.SID=A.SID AND B.LID=A.LID WHERE B.SID IS NULL
			
		----- FINAL FORMATTING
		DROP TABLE IF EXISTS #FINAL
		SELECT A.* INTO #FINAL FROM (
			SELECT C.VID, A.T,
				CASE
					WHEN A.O < 0 THEN 0
					WHEN A.O > A.W THEN A.W
					ELSE A.O
				END AS O
				FROM #BOTH_OPTS_OUTLIERS_UL A
			INNER JOIN SPX_VERT.SPX.VERT C ON C.SID=A.SID AND C.LID=A.LID
			WHERE CAST(A.T AS TIME(0)) <= '13:00' AND OI <> 1
		) A LEFT JOIN SPX_VERT.SPX.VERT_TS B ON B.VID=A.VID AND B.T=A.T WHERE B.VID IS NULL
			
		INSERT INTO SPX_VERT.SPX.VERT_TS
		SELECT X.* FROM (
			SELECT VID, T, MAX(O) O, MAX(AVG_R) AVG_R FROM (
				SELECT F.*,
						CAST(
							AVG(O) OVER(PARTITION BY F.VID ORDER BY F.T ASC ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) 
						AS DECIMAL(9,2)) AVG_R
				FROM #FINAL F
			) Z GROUP BY VID, T
		) X
		LEFT JOIN SPX_VERT.SPX.VERT_TS TS ON TS.T=X.T AND TS.VID=X.VID
		WHERE TS.T is NULL
			
	END
	ELSE
	BEGIN
		Print 'MISSING DAILY DATA FOR' + CAST(@Expiry AS VARCHAR(30))
	END
END
111