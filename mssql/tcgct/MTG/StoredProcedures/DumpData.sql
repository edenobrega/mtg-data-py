CREATE PROCEDURE [MTG].[DumpData]
as
    DELETE FROM [MTG].[CardPart]
    DELETE FROM [MTG].[CardFace]
    DELETE FROM [MTG].[TypeLine]
    DELETE FROM [MTG].[Card]
    DELETE FROM [MTG].[Layout]
    DELETE FROM [MTG].[Rarity]
    DELETE FROM [MTG].[Set]
    DELETE FROM [MTG].[SetType]
    DELETE FROM [MTG].[CardType]

    DBCC CHECKIDENT ('[MTG].[CardPart]', RESEED, 0);
    DBCC CHECKIDENT ('[MTG].[Card]', RESEED, 0);
    DBCC CHECKIDENT ('[MTG].[CardFace]', RESEED, 0);
    DBCC CHECKIDENT ('[MTG].[Layout]', RESEED, 0);
    DBCC CHECKIDENT ('[MTG].[Rarity]', RESEED, 0);
    DBCC CHECKIDENT ('[MTG].[Set]', RESEED, 0);
    DBCC CHECKIDENT ('[MTG].[SetType]', RESEED, 0);
    DBCC CHECKIDENT ('[MTG].[CardType]', RESEED, 0);
GO

