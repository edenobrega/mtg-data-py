CREATE TABLE [Collection].[CollectedCards_MTG] (
    [UserID] INT NOT NULL,
    [CardID] INT NOT NULL,
    [Count]  INT NOT NULL
);

GO

CREATE INDEX [IX_CollectedCards_UserID] ON [Collection].[CollectedCards_MTG] ([UserID])
