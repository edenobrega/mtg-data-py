CREATE     procedure [MTG].[DumpData]
as
    delete from mtg.CardPart
    delete from mtg.CardFace
    delete from mtg.TypeLine
    delete from mtg.Card
    delete from mtg.Layout
    delete from mtg.Rarity
    delete from mtg.[Set]
    delete from mtg.SetType
    delete from mtg.CardType

    DBCC CHECKIDENT ('[MTG].[CardPart]', RESEED, 0);
    DBCC CHECKIDENT ('[MTG].[Card]', RESEED, 0);
    DBCC CHECKIDENT ('[MTG].[CardFace]', RESEED, 0);
    DBCC CHECKIDENT ('[MTG].[Layout]', RESEED, 0);
    DBCC CHECKIDENT ('[MTG].[Rarity]', RESEED, 0);
    DBCC CHECKIDENT ('[MTG].[Set]', RESEED, 0);
    DBCC CHECKIDENT ('[MTG].[SetType]', RESEED, 0);
    DBCC CHECKIDENT ('[MTG].[CardType]', RESEED, 0);
GO

