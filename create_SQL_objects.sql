USE [Learning]
GO
/****** Object:  Table [dbo].[CreditCardInfo]    Script Date: 11/13/2024 6:09:07 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CreditCardInfo](
	[RecordID] [varchar](255) NULL,
	[BankName] [varchar](50) NOT NULL,
	[CardLast4Digits] [int] NOT NULL,
	[BillDate] [date] NOT NULL,
	[TotalAmountDue] [decimal](10, 2) NOT NULL,
	[RecordCreatedDate] [datetime] NULL,
	[RecordModifiedDate] [datetime] NULL,
 CONSTRAINT [PK_CreditCardInfo] PRIMARY KEY CLUSTERED 
(
	[BankName] ASC,
	[CardLast4Digits] ASC,
	[BillDate] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Operation_Watermark]    Script Date: 11/13/2024 6:09:07 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Operation_Watermark](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[OperationName] [varchar](50) NULL,
	[LastUpdatedTimestamp] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Staging_CreditCardInfo]    Script Date: 11/13/2024 6:09:07 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Staging_CreditCardInfo](
	[BankName] [varchar](50) NULL,
	[CardLast4Digits] [int] NULL,
	[BillDate] [date] NULL,
	[TotalAmountDue] [decimal](10, 2) NULL,
	[RecordCreatedDate] [datetime] NULL,
	[RecordModifiedDate] [datetime] NULL
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[CreditCardInfo] ADD  CONSTRAINT [DF_CreditCardInfo_GUID]  DEFAULT (newid()) FOR [RecordID]
GO
ALTER TABLE [dbo].[CreditCardInfo] ADD  CONSTRAINT [DF_CreditCardInfo_RecordCreatedDate]  DEFAULT (getutcdate()) FOR [RecordCreatedDate]
GO
ALTER TABLE [dbo].[CreditCardInfo] ADD  CONSTRAINT [DF_CreditCardInfo_RecordModifiedDate]  DEFAULT (getutcdate()) FOR [RecordModifiedDate]
GO
ALTER TABLE [dbo].[Staging_CreditCardInfo] ADD  CONSTRAINT [DF_Staging_CreditCardInfo_RecordCreatedDate]  DEFAULT (getutcdate()) FOR [RecordCreatedDate]
GO
ALTER TABLE [dbo].[Staging_CreditCardInfo] ADD  CONSTRAINT [DF_Staging_CreditCardInfo_RecordModifiedDate]  DEFAULT (getutcdate()) FOR [RecordModifiedDate]
GO
/****** Object:  StoredProcedure [dbo].[usp_Update_CreditCardInfo_from_Staging]    Script Date: 11/13/2024 6:09:07 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [dbo].[usp_Update_CreditCardInfo_from_Staging]
AS
BEGIN
	MERGE dbo.CreditCardInfo tgt
	USING dbo.Staging_CreditCardInfo src
	ON tgt.BankName = src.BankName AND tgt.CardLast4Digits = src.CardLast4Digits AND tgt.BillDate = src.BillDate
	WHEN MATCHED THEN
		UPDATE
		SET 
		tgt.TotalAmountDue = src.TotalAmountDue,
		tgt.RecordModifiedDate = src.RecordModifiedDate
	WHEN NOT MATCHED THEN
		INSERT (BankName, CardLast4Digits, BillDate, TotalAmountDue, RecordCreatedDate, RecordModifiedDate)
		VALUES (src.BankName, src.CardLast4Digits, src.BillDate, src.TotalAmountDue, src.RecordCreatedDate, src.RecordModifiedDate);

END
GO

SET IDENTITY_INSERT [dbo].[Operation_Watermark] ON 
GO
INSERT [dbo].[Operation_Watermark] ([ID], [OperationName], [LastUpdatedTimestamp]) VALUES (1, N'CreditCardReader', GETUTCDATE() -45)
GO
SET IDENTITY_INSERT [dbo].[Operation_Watermark] OFF
GO


