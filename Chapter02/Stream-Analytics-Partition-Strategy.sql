-- Create a user-defined function to partition the data

CREATE FUNCTION MyPartitioningFunction()
RETURNS @result TABLE 
(
    PartitionKey nvarchar(100),
    PartitionId int
)
WITH SCHEMABINDING
AS
BEGIN
    INSERT INTO @result
    SELECT
        DeviceId As PartitionKey, -- your partition key column
        HASHBYTES('MD5', CAST(DeviceId AS nvarchar(MAX))) % 10 AS PartitionId -- Assuming 10 partitions
    FROM
        input;
    RETURN;
END;

-- you would use the partition function in your query to process the data

SELECT
    DeviceId,
    AVG(Temperature) AS AvgTemperature, 
    COUNT(*) AS MessageCount
INTO 
    output
FROM
    input
GROUP BY
    DeviceId,
    TumblingWindow(minute, 5),
    MyPartitioningFunction().PartitionId; 
