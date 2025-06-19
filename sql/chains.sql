
USE [CHAINS]
GO


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[SPX_CHAIN](
	[CP] [smallint] NULL,
	[Expiry] [date] NULL,
	[DTE] [int] NULL,
	[Strike] [int] NULL,
	[Bid] [decimal](9, 2) NULL,
	[Ask] [decimal](9, 2) NULL,
	[Volume] [int] NULL,
	[DTime] [datetime2](0) NULL,
	[Volatility] [decimal](9, 2) NULL,
	[Delta] [decimal](9, 3) NULL,
	[Gamma] [decimal](9, 3) NULL,
	[Theta] [decimal](9, 3) NULL,
	[Vega] [decimal](9, 3) NULL,
	[Rho] [decimal](9, 3) NULL,
	[OI] [int] NULL,
	[Weekly] [int] NULL
) ON [PRIMARY]
GO


CREATE NONCLUSTERED INDEX [IX_SPX_CHAIN_DTime] ON [dbo].[SPX_CHAIN]
(
	[DTime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO


