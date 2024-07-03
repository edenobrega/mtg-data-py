CREATE TABLE [MTG].[CollectedCards] (
    [UserID] INT NOT NULL,
    [CardID] INT NOT NULL,
    [Count]  INT NOT NULL
);

GO

CREATE INDEX [IX_CollectedCards_UserID] ON [MTG].[CollectedCards] ([UserID])

