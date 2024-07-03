CREATE PROCEDURE [MTG].[UpdateMTGCollection]
    @userID INT,
    @json NVARCHAR(MAX)

AS
BEGIN
    SET NOCOUNT ON

    DROP TABLE IF EXISTS #temp

    SELECT 
        JSON_VALUE(j.value, '$[0]') AS [SetCode], 
        JSON_VALUE(j.value, '$[1]') AS [Card], 
        JSON_VALUE(j.value, '$[2]') AS [Change]
    INTO #temp
    FROM OPENJSON(@json, '$.ids') AS j

    DECLARE @existing INT = (
        SELECT COUNT(1)
        FROM #temp AS t
        JOIN [MTG].[Set] AS s ON s.[shorthand] = t.[SetCode]
        JOIN [MTG].[Card] AS c ON c.[card_set_id] = s.[id] AND c.[collector_number] = t.[Card]
    )

    IF @existing != (SELECT COUNT(1) FROM #temp)
    BEGIN;
        SELECT 0
        RETURN
    END

    MERGE INTO [MTG].[CollectedCards] AS ccm
    USING (
            SELECT c.id AS [CardID], [Change]
            FROM #temp AS t
            JOIN [MTG].[Set] AS s ON s.[shorthand] = t.[SetCode]
            JOIN [MTG].[Card] AS c ON c.[card_set_id] = s.[id] AND c.[collector_number] = t.[Card]
        ) AS cv ON ccm.UserID = @userID AND cv.CardID = ccm.CardID
    WHEN MATCHED THEN
        UPDATE SET [Count] = [Count] + cv.[Change]
    WHEN NOT MATCHED THEN
        INSERT ([UserID], [CardID], [Count])
        VALUES (@userID, cv.[CardID], cv.[Change]);

    DELETE FROM [MTG].[CollectedCards]
    WHERE [Count] < 1

    SELECT 1
END
GO

