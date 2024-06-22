CREATE TABLE [TCGCT].[Settings] (
    [ID]     INT              NOT NULL,
    [GameID] INT              NOT NULL,
    [UserID] UNIQUEIDENTIFIER NOT NULL,
    [Key]    NVARCHAR (100)   NOT NULL,
    [Value]  NVARCHAR (1000)  NULL
);
GO

