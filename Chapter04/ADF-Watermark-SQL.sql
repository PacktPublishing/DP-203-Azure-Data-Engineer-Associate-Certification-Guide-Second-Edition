-- Watermarks Example Code for ADF.

-- Create a FactTrips Table

CREATE TABLE FactTrips 
(
TripIDF INT,
CustomerID INT,
LastModifiedTime DATETIME2
);

-- Insert Sample Values To The FactTrips Table.

INSERT INTO [dbo].[FactTrips] values (100, 200, CURRENT_TIMESTAMP);
INSERT INTO [dbo].[FactTrips] values (101, 201, CURRENT_TIMESTAMP);
INSERT INTO [dbo].[FactTrips] values (102, 202, CURRENT_TIMESTAMP);

-- Create a Watermark Table
DROP TABLE WatermarkTable;

CREATE TABLE WatermarkTable
(
  [TableName] VARCHAR(100),
  [WatermarkValue] DATETIME,
);

INSERT INTO [dbo].[WatermarkTable] VALUES ('FactTrips', CURRENT_TIMESTAMP);
SELECT * FROM WatermarkTable;
GO

-- You can either update the Watermark table manually as shown or create a stored procedure and execute it everytime there is an update
UPDATE [dbo].[WatermarkTable] SET [WatermarkValue] = CURRENT_TIMESTAMP WHERE [TableName] = 'FactTrips';


-- Create a SP to update WatermarkTable automatically when the new data comes in.

DROP PROCEDURE uspUpdateWatermark
GO

CREATE PROCEDURE [dbo].uspUpdateWatermark @LastModifiedtime DATETIME, @TableName VARCHAR(100)
AS
BEGIN
UPDATE [dbo].[WatermarkTable] 
SET [WatermarkValue] = @LastModifiedtime 
WHERE [TableName] = @TableName
END
GO

-- Executing the stored procedure
DECLARE @timestamp AS DATETIME = CURRENT_TIMESTAMP;
EXECUTE uspUpdateWatermark @LastModifiedtime=@timestamp, @TableName='FactTrips';

SELECT * FROM WatermarkTable;

-- Run the Query to check the LastModifiedTime in the source table (FactTrips)
SELECT MAX(LastModifiedTime) AS NewWatermarkValue FROM FactTrips;
