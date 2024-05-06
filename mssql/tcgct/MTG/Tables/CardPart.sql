CREATE TABLE [MTG].[CardPart] (
    [ID]            BIGINT        IDENTITY (1, 1) NOT NULL,
    [CardID]        BIGINT        NULL,
    [Object]        VARCHAR (25)  NULL,
    [Component]     VARCHAR (100) NULL,
    [RelatedOracleID] NVARCHAR(36)  NULL
);
GO

ALTER TABLE [MTG].[CardPart]
    ADD CONSTRAINT [PK_MTG.CardPart] PRIMARY KEY CLUSTERED ([ID] ASC);
GO

ALTER TABLE [MTG].[CardPart]
    ADD CONSTRAINT [FK_CardPart_Card] FOREIGN KEY ([CardID]) REFERENCES [MTG].[Card] ([id]);
GO

