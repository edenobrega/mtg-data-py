CREATE   view [MTG].[TableCounts] as 
select *
from (
select 'Cards' as [Table], count(*) as Count
from MTG.[Card] with (nolock)

union

select 'Card Faces', count(*)
from MTG.CardFace with (nolock)

union

select 'Card Part', count(*)
from MTG.CardPart with (nolock)

union

select 'Card Type', count(*)
from MTG.CardType with (nolock)

union

select 'Rarity', count(*)
from MTG.Rarity with (nolock)

union

select 'Set', count(*)
from MTG.[Set] with (nolock)

union

select 'Set Type', count(*)
from MTG.SetType with (nolock)

union

select 'TypeLine', count(*)
from MTG.TypeLine with (nolock)
) as d
GO

