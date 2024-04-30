-- Data Partition Switching Example

-- Let’s assume that we need to store only 3 months’ worth of data. 
-- Our Fact table, dbo.TripTable, contains the data for 20240101, 20240201, and 20240301. 
-- Now, let’s learn how to delete the first month’s data and add the new month’s data, 20240401, to the table using the technique of Data switching

-- Since Synapse SQL doesn't support CREATE TABLE IF EXISTS, I've provided commented DROP TABLE statements for convenience
-- DROP TABLE dbo.TripTable

-- Create a sample TripTable with partitions
CREATE TABLE dbo.TripTable
(
    [tripId] INT NOT NULL,
    [driverId] INT NOT NULL,
    [customerId] INT NOT NULL,
    [tripDate] INT,
    [startLocation] VARCHAR (40),
    [endLocation] VARCHAR (40)
 )
 WITH
 (
    CLUSTERED COLUMNSTORE INDEX,
    DISTRIBUTION = HASH ([tripId]),
    PARTITION ([tripDate] RANGE RIGHT FOR VALUES
        ( 20240101, 20240201, 20240301 )
  )
)

-- Insert some dummy data covering all the three date partition ranges
INSERT INTO dbo.TripTable VALUES (100, 200, 300, 20240101, 'New York', 'New Jersey');
INSERT INTO dbo.TripTable VALUES (101, 201, 301, 20240101, 'Miami', 'Dallas');
INSERT INTO dbo.TripTable VALUES (102, 202, 302, 20240102, 'Phoenix', 'Tempe');
INSERT INTO dbo.TripTable VALUES (103, 203, 303, 20240204, 'LA', 'San Jose');
INSERT INTO dbo.TripTable VALUES (104, 204, 304, 20240205, 'Seattle', 'Redmond');
INSERT INTO dbo.TripTable VALUES (105, 205, 305, 20240301, 'Atlanta', 'Chicago');

SELECT * FROM dbo.TripTable;

-- Deleting an old partition
-- To delete a partition, we need to create a dummy table that has the same structure as the original table 
-- and then swap out the partition from the original table to  the dummy table. 
-- This section will show you how to switch out the 20240101 partition.

-- Create a dummy table that contains the partition that needs to be switched out, as follows:
-- DROP TABLE dbo.TripTable_20240101;
CREATE TABLE dbo.TripTable_20240101
WITH
 (
    CLUSTERED COLUMNSTORE INDEX,
    DISTRIBUTION = HASH ([tripId]),
    PARTITION ([tripDate] RANGE RIGHT FOR VALUES (20240101) ) 
  )
AS
SELECT * FROM dbo.TripTable WHERE 1=2 ;

SELECT * FROM dbo.TripTable_20240101;

-- Let us try to switch out PARTITION 20240101.
-- 20240101 is actually the second partition since the first partition will correspond to all the values before 20240101, 
-- which, in our case, would be empty. 
ALTER TABLE dbo.TripTable SWITCH PARTITION 2 TO dbo.TripTable_20240101 PARTITION 2 WITH (TRUNCATE_TARGET = ON);

-- Now, dbo.TripTable will contain 0 rows for partition 2, which corresponds to 20240101.

-- Next, let’s add a new partition, 20240401, to the table.
-- We have to repeat the same steps as before to empty the partition 20240301 first.
-- Then split the last partition of TripTable into two ranges: 20240301 and 20240401. 
-- Move the 20240301 data from the temporary table back into TripTable
-- Finally swap in a new partition for 20240401

-- Create a dummy table that with the same partitions as the TripTable:
DROP TABLE dbo.TripTable_20240301;
CREATE TABLE dbo.TripTable_20240301
WITH
 (
    CLUSTERED COLUMNSTORE INDEX,
    DISTRIBUTION = HASH ([tripId]),
    PARTITION ([tripDate] RANGE RIGHT FOR VALUES
        (20240101, 20240201, 20240301)
    )
  )
AS
SELECT * FROM dbo.TripTable WHERE
   [tripDate] >= 20240301 AND [tripDate] < 20240401 ;

SELECT * FROM dbo.TripTable_20240301;

-- Now switch out PARTITION 20240103.

-- Run the ALTER TABLE command, as shown in the following code block, to swap the partition out:
ALTER TABLE dbo.TripTable SWITCH PARTITION 4 TO dbo.TripTable_20240301 PARTITION 4 WITH (TRUNCATE_TARGET = ON);

-- Adding a new partition to the TripTable
-- To add our new partition, we need to split the last partition into two partitions. We can use the following SPLIT command to do this:
ALTER TABLE dbo.TripTable SPLIT RANGE (20240401);

-- Copy back the 20240301 data now. 
-- Since the partition ranges should be exactly the same as TripTable, which now contains one extra 20240401 partition,
-- we have to create another temporary table with all the four partition ranges as shown by copying the data from the TripTable_20240301 temporary table.

-- DROP TABLE dbo.TripTable_20240301_20240301;
CREATE TABLE dbo.TripTable_20240301_20240301
WITH
 (
    CLUSTERED COLUMNSTORE INDEX,
    DISTRIBUTION = HASH ([tripId]),
    PARTITION ([tripDate] RANGE RIGHT FOR VALUES
        (20240101, 20240201, 20240301, 20240401)
    )
  )
AS
SELECT * FROM dbo.TripTable_20240301 WHERE
   [tripDate] >= 20240301 AND [tripDate] < 20240401 ;

SELECT * FROM dbo.TripTable_20240301_20240301;

-- Now switch back TripTable_20240301 (using TripTable_20240301_20240301 temp table) data into TripTable

ALTER TABLE dbo.TripTable_20240301_20240301 SWITCH PARTITION 4 TO dbo.TripTable PARTITION 4 WITH (TRUNCATE_TARGET = ON);

-- DROP TABLE dbo.TripTable_new
-- Once we have created the new partition for 20240401, we must create a dummy table with the same partition alignment again to add the new month's data
-- The following code snippet does this:
CREATE TABLE dbo.TripTable_new
WITH
 (
    CLUSTERED COLUMNSTORE INDEX,
    DISTRIBUTION = HASH ([tripId]),
    PARTITION ([tripDate] RANGE RIGHT FOR VALUES
        (20240101, 20240201, 20240301, 20240401)
  )
)
AS
SELECT * FROM dbo.TripTable WHERE 1 = 2;

-- Let’s add some values to the new partition:
INSERT INTO dbo.TripTable_new VALUES (333, 444, 555, 20240401, 'New York', 'New Jersey');

SELECT * FROM dbo.TripTable_new;

-- Once we have loaded the partition data into the dummy table, we can switch the partition into our Fact table using the ALTER command

ALTER TABLE dbo.TripTable_new SWITCH PARTITION 5 TO dbo.TripTable PARTITION 5 WITH (TRUNCATE_TARGET = ON);

-- The ALTER TABLE commands will return almost immediately as they are metadata operations  
-- It doesn't involve copying rows from one partition to another. 

-- Check the data to confirm if the swap has happened correctly

SELECT * FROM dbo.TripTable;