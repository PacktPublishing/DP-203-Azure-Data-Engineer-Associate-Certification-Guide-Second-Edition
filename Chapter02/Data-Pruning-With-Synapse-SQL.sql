-- Dedicated SQL pool example with pruning

-- The following table has been partitioned using the PARTITION keyword on tripDate.

CREATE TABLE dbo.TripTable
(
    [tripId] INT NOT NULL,
    [driverId] INT NOT NULL,
    [customerId] INT NOT NULL,
    [tripDate] INT,
    [startLocation] VARCHAR(40),
    [endLocation] VARCHAR(40)
 )
 WITH
 (
    CLUSTERED COLUMNSTORE INDEX,
    DISTRIBUTION = HASH ([tripId]),
    PARTITION ([tripDate] RANGE RIGHT FOR VALUES
        ( 20240101, 20240201, 20240301 )
    )
)
GO

INSERT INTO dbo.TripTable VALUES (100, 200, 300, 20240101, 'New York', 'New Jersey');
INSERT INTO dbo.TripTable VALUES (101, 201, 301, 20240101, 'Miami', 'Dallas');
INSERT INTO dbo.TripTable VALUES (102, 202, 302, 20240102, 'Phoenix', 'Tempe');
INSERT INTO dbo.TripTable VALUES (103, 203, 303, 20240204, 'LA', 'San Jose');
INSERT INTO dbo.TripTable VALUES (104, 204, 304, 20240205, 'Seattle', 'Redmond');
INSERT INTO dbo.TripTable VALUES (105, 205, 305, 20240301, 'Atlanta', 'Chicago');

-- If find all the customers who traveled with IAC in the month of January. 
-- All you need to do is use a simple filter, such as in the following example:
-- Tis will ensure that only the data in the below partitions are read and not a 
-- full table scan.

SELECT customerId FROM TripTable WHERE tripDate BETWEEN '20240101' AND '20240131'
