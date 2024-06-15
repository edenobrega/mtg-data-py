CREATE TABLE [Collection].[CollectedCards_MTG]
(
	[Id] INT NOT NULL PRIMARY KEY, 
    [UserID] UNIQUEIDENTIFIER NOT NULL, 
    [CardID] INT NOT NULL
)

GO

CREATE INDEX [IX_CollectedCards_UserID] ON [Collection].[CollectedCards_MTG] ([UserID])
