-- Synapse SQL PolyBase Example To Load Data From External Location.

IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'Dp203ParquetFormat') 
	CREATE EXTERNAL FILE FORMAT [Dp203ParquetFormat] 
	WITH ( FORMAT_TYPE = PARQUET)

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'Dp203DataSource') 
	CREATE EXTERNAL DATA SOURCE [Dp203DataSource] 
	WITH (
		LOCATION = '<INSERT abfss://  DATA SOURCE LOCATION>' 
	)

-- Here we are assuming that the parquet files are present in /partition/year=2022/*/*/
CREATE EXTERNAL TABLE TripExtTable
WITH (
	LOCATION = '/partition/year=2022/*/*/*.parquet',
	DATA_SOURCE = [Dp203DataSource],
	FILE_FORMAT = [Dp203ParquetFormat]
) AS 
SELECT 
	[tripId] INT,
	[driverId] INT,
	[customerId] INT,
	[cabId] INT,
	[tripDate] INT,
	[startLocation] VARCHAR (50),
	[endLocation] VARCHAR (50)
FROM 
    OPENROWSET(BULK '/partition/year=2022/*/*/*.parquet', FORMAT='PARQUET')

-- Copy the data from the external table into an actual SQL table.
CREATE TABLE TripsProdTable
WITH
(
    CLUSTERED COLUMNSTORE INDEX,
    DISTRIBUTION = ROUND_ROBIN
)
AS
SELECT * FROM TripsExtTable
