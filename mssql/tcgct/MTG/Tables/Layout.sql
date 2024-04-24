CREATE TABLE [MTG].[Layout] (
    [id]   INT            IDENTITY (1, 1) NOT NULL,
    [name] NVARCHAR (100) NOT NULL
);
GO

ALTER TABLE [MTG].[Layout]
    ADD CONSTRAINT [PK__Layout] PRIMARY KEY CLUSTERED ([id] ASC);
GO

