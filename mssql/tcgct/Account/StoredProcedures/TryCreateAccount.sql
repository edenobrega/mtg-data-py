CREATE PROCEDURE [Account].[TryCreateAccount]
    @Username NVARCHAR(250),
    @Password NVARCHAR(MAX)
AS
BEGIN
    IF EXISTS(SELECT 1 FROM [Account].[User] WHERE [Username] = @Username)
    BEGIN
        SELECT 0
        RETURN
    END

    DECLARE @newUserID TABLE (userID INT) 

    INSERT INTO [Account].[User]([Username], [Password]) 
    OUTPUT inserted.ID INTO @newUserID
    VALUES (@Username, @Password)

    DECLARE @userID INT = (SELECT TOP 1 userID FROM @newUserID)

    EXECUTE [TCGCT].[CreateDefaultSettings] @userID

    SELECT 1
END
GO