CREATE PROCEDURE MTG.StringAggGroupExample
as
SELECT 
    c.[name] as [Card Name]
    ,STRING_AGG(ct.name, ' ') WITHIN GROUP (ORDER BY [Order] asc) as hngh
FROM [MTG].[Card] as c
join mtg.TypeLine as tl on tl.card_id = c.id
join mtg.CardType as ct on ct.id = tl.type_id
group by c.name, c.source_id
GO

