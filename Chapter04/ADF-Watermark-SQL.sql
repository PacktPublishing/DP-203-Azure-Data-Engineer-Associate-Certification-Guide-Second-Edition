-- Watermarks Example Code for ADF.

-- Create a FactTrips Table

CREATE TABLE FactTrips 
(
TripIDF INT,
customerID INT,
LastModifiedTime DATETIME
);

-- Insert Sample Values To The FactTrips Table.

INSERT INTO [dbo].[FactTrips] values (100, 200, CURRENT_TIMESTAMP);
INSERT INTO [dbo].[FactTrips] values (101, 201, CURRENT_TIMESTAMP);
INSERT INTO [dbo].[FactTrips] values (102, 202, CURRENT_TIMESTAMP);

-- Create a Watermark Table

CREATE TABLE WatermarkTable
(
  [TableName] VARCHAR(100),
  [WatermarkValue] DATETIME,
);


-- Create a SP to update WatermarkTable automatically when the new data comes in.

CREATE PROCEDURE [dbo].uspUpdateWatermark @LastModifiedtime DATETIME, @TableName VARCHAR(100)
AS
BEGIN
UPDATE [dbo].[WatermarkTable] 
SET [WatermarkValue] = @LastModifiedtime 
WHERE [TableName] = @TableName
END

-- Run the Query to check the LastModifiedTime in the source table (FactTrips)

SELECT MAX(LastModifiedTime) AS NewWatermarkValue FROM FactTrips;
