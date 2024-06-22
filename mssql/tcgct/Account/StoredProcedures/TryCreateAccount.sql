CREATE PROCEDURE [Account].[TryCreateAccount]
AS
BEGIN
    IF EXISTS(SELECT 1 FROM [Account].[User] WHERE [Username] = @Username)
    BEGIN
        SELECT 0
        RETURN
    END

    INSERT INTO [Account].[User]([Username], [Password]) VALUES (@Username, @Password)

    SELECT 1
END
GO

