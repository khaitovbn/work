USE InfoDB;
go


SELECT *
FROM dbo.tbOtgr_Pall_

SELECT *
FROM dict.DC



SELECT DISTINCT s.ID, s.SHORT_NAME_NM
	 , d.EXC_CODE, d.FULL_NAME
FROM kpi.KPI_DC as s
INNER JOIN KPI.KPI_DC_LEVEL AS a
	ON s.ID = a.DC_ID
	AND a.USE_TYPE_ID = 4
INNER JOIN dbo.tbDCs AS d
	ON s.SHORT_NAME_NM = d.SHORT_NAME
ORDER BY SHORT_NAME_NM, d.EXC_CODE

SELECT *
FROM dbo.tbDCs ORDER BY FULL_NAME

SELECT *
FROM KPI.KPI_DC_LEVEL
WHERE USE_TYPE_ID = 4

SELECT * FROM tbDCs

--	SELECT * FROM dbo.tbOtgr_Pall_
DELETE FROM [InfoDB_p].kpi.[tbOtgr_Pall_DEL];
INSERT INTO [InfoDB_p].kpi.[tbOtgr_Pall_DEL]
SELECT *
FROM dbo.tbOtgr_Pall_
WHERE IsNull(DROPPREPARED, BEG_TASK) >= '2020-01-01' 
	AND IsNull(DROPPREPARED, BEG_TASK) < '2020-04-01' 
	AND DC IN ( 
SELECT DISTINCT TOP 3 d.EXC_CODE
FROM kpi.KPI_DC as s
INNER JOIN KPI.KPI_DC_LEVEL AS a
	ON s.ID = a.DC_ID
	AND a.USE_TYPE_ID = 4
INNER JOIN dbo.tbDCs AS d
	ON s.SHORT_NAME_NM = d.SHORT_NAME)


SELECT *
FROM [InfoDB_p].kpi.[tbOtgr_Pall_DEL]


SELECT *
FROM infodb_p.[KPI].[KPI_NAME]


USE [InfoDB_p]
GO

DROP TABLE [InfoDB_p].kpi.[tbOtgr_Pall_DEL];
CREATE TABLE kpi.[tbOtgr_Pall_DEL](
	[ID] [bigint] NOT NULL,
	[FROM_DMY] [datetime] NULL,
	[DC] [varchar](5) NOT NULL,
	[DROPID] [varchar](50) NOT NULL,
	[TOID] [varchar](255) NOT NULL,
	[CARTONTYPE] [varchar](50) NOT NULL,
	[USERKEY] [varchar](50) NOT NULL,
	[STORECODE] [varchar](255) NULL,
	[ORDERKEY] [varchar](50) NOT NULL,
	[CASES] [int] NOT NULL,
	[WGT] [float] NULL,
	[M3] [float] NULL,
	[QTY_SKU] [int] NULL,
	[DMY_CREATED] [datetime] NOT NULL,
	[BEG_TASK] [datetime] NULL,
	[MIN_PICK] [datetime] NULL,
	[MAX_PICK] [datetime] NULL,
	[PRINT_YARL] [datetime] NULL,
	[DROPPREPARED] [datetime] NULL,
	[DROPLOADED] [datetime] NULL,
	[DROPSHIPPED] [datetime] NULL,
	[LOC] [varchar](50) NULL,
	[ID_LINE] [varchar](50) NULL,
	[IS_CARGO] [varchar](50) NULL,
	[FULLPA] [int] NULL,
	[SSCC] [varchar](50) NULL,
	[XD] [varchar](50) NULL,
	[BC_DROPID] [varchar](50) NULL,
	[C_LOC] [int] NULL,
	[SKIP_LOC] [int] NULL,
	[PCC_INTERVAL] [varchar](255) NULL,
	[CARTONGROUP] [varchar](255) NOT NULL,
	[FINAL_CLIENT] [varchar](50) NULL,
	[DMY] [date] NOT NULL,
	[TRUCKLISTPRINTED] [datetime] NULL,
	[ORDER_TYPE] [varchar](50) NULL,
	[REQUESTSHIPDATE] [datetime] NULL,
	[TPP] [int] NULL,
	[SEC_BOX] [int] NULL,
	[TP_CORRECTION] [float] NULL,
 CONSTRAINT [PK_tbOtgr_Pall_2] PRIMARY KEY CLUSTERED 
(
	[DC] ASC,
	[DROPID] ASC,
	[TOID] ASC,
	[ORDERKEY] ASC,
	[CARTONGROUP] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO



