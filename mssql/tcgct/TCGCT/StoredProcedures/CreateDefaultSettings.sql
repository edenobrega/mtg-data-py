CREATE PROCEDURE [TCGCT].[CreateDefaultSettings]
  @userID INT
AS
BEGIN
  DECLARE @mtgID INT = (SELECT ID FROM [TCGCT].[Games] WHERE [Name] = 'MTG')

  INSERT INTO [TCGCT].[Settings]([GameID], [UserID], [Key], [Value]) VALUES 
  (@mtgID, @userID, 'FilterBySetIDs', NULL),
  (@mtgID, @userID, 'FilterBySetTypes', NULL),
  (@mtgID, @userID, 'CollectingSets', NULL)
END