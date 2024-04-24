CREATE TABLE [MTG].[CardType] (
    [id]   BIGINT         IDENTITY (1, 1) NOT NULL,
    [name] NVARCHAR (300) NULL,
    PRIMARY KEY CLUSTERED ([id] ASC)
);
GO

