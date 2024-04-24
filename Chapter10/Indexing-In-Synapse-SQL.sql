-- Creating a table with clustered columnstore index
CREATE TABLE dbo.Driver 
( 
[driverId] INT NOT NULL, 
[name] VARCHAR(40)
 )
 WITH ( CLUSTERED COLUMNSTORE INDEX   );	

-- Creating a table with clustered index:  
CREATE TABLE dbo.Driver 
( 
[driverId] INT NOT NULL, 
[name] VARCHAR(40),
[customerId] INT NOT NULL
 )
 WITH ( CLUSTERED INDEX (driverId  ));	

 -- Add a non-clustered index on the Driver table
 
 CREATE INDEX customerIdIndex on dbo.Driver (customerId  );

 -- Loading data to Heap (Non-index) table.
 CREATE TABLE dbo.Driver 
( 
[driverId] INT NOT NULL, 
[name] VARCHAR(40)
 )
 WITH ( HEAP   );	

