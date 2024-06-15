CREATE PROCEDURE [MTG].[StringAggGroupExample]
as
SELECT 
    c.[name] AS [Card Name]
    ,STRING_AGG(ct.[name], ' ') WITHIN GROUP (ORDER BY [order] ASC) AS TypeLine
FROM [MTG].[Card] AS c
JOIN [MTG].[TypeLine] AS tl ON tl.[card_id] = c.[id]
JOIN [MTG].[CardType] AS ct ON ct.[id] = tl.[type_id]
GROUP BY c.[name], c.[source_id]
GO

