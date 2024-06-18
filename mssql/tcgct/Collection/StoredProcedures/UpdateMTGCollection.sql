CREATE PROCEDURE Collection.UpdateMTGCollection
    @userID INT,
    @json NVARCHAR(MAX)

AS
BEGIN
    DROP TABLE IF EXISTS #temp

    SELECT 
        JSON_VALUE(j.value, '$[0]') AS [SetCode], 
        JSON_VALUE(j.value, '$[1]') AS [Card], 
        JSON_VALUE(j.value, '$[2]') AS [Change]
    INTO #temp
    FROM OPENJSON(@json, '$.ids') AS j

    MERGE INTO [Collection].[CollectedCards_MTG] AS ccm
    USING (
            SELECT c.ID AS [CardID], [Change]
            FROM #temp AS t
            JOIN MTG.[Set] AS s ON s.shorthand = t.SetCode
            JOIN MTG.[Card] AS c ON c.card_set_id = s.ID AND c.collector_number = t.Card
        ) AS cv ON ccm.UserID = @userID AND cv.CardID = ccm.CardID
    WHEN MATCHED THEN
        UPDATE SET [Count] = [Count] + cv.[Change]
    WHEN NOT MATCHED THEN
        INSERT ([UserID], [CardID], [Count])
        VALUES (@userID, cv.[CardID], cv.[Change]);

    DELETE FROM [Collection].[CollectedCards_MTG]
    WHERE [Count] < 1
END
GO
