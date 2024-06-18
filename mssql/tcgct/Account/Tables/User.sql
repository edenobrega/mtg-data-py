CREATE TABLE [Account].[User] (
    [ID]       INT              IDENTITY (1, 1) NOT NULL,
    [UID]      UNIQUEIDENTIFIER CONSTRAINT [DEFAULT_User_ID] DEFAULT (newid()) NOT NULL,
    [Username] NVARCHAR (250)   NOT NULL,
    [Password] NVARCHAR (MAX)   NOT NULL,
    PRIMARY KEY CLUSTERED ([ID] ASC, [UID] ASC)
);
GO
