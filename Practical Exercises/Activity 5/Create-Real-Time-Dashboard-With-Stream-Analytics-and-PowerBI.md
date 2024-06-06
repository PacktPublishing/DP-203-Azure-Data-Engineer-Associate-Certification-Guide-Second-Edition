# Create a Real-Time Dashboard with Azure Stream Analytics and Microsoft Power BI

## View The Streaming Data Source
Before creating an Azure Stream Analytics job to process real-time data, let's take a look at the **Imaginary Airport Cab (IAC)** JSON source data.

https://github.com/PacktPublishing/DP-203-Azure-Data-Engineer-Associate-Certification-Guide-Second-Edition/blob/main/Practical%20Exercises/Activity%203/Data/IAC-TripData.json


## Before You Start

You'll also need access to the Microsoft Power BI service. Your school or organization email or you can [sign up for the Power BI service as an individual](https://learn.microsoft.com/power-bi/fundamentals/service-self-service-signup-for-power-bi).


## Create a Query to Summarize the IAC-TripData Stream

1. View the **Query** page for the Stream Analytics job.

2. Modify the query as follows:

    ```
    SELECT
        DateAdd(second,-5,System.TimeStamp) AS StartTime,
        System.TimeStamp AS EndTime,
        driverId,
        SUM(tripAmount) AS TripAmount
    INTO
        [PowerBI-Dataset]
    FROM
        [IACTripEventHub] TIMESTAMP BY EventEnqueuedUtcTime
    GROUP BY driverId, TumblingWindow(second, 5)
    HAVING COUNT(*) > 1
    ```

    Observe that this query uses the **System.Timestamp** (based on the **EventEnqueuedUtcTime** field) to define the start and end of each 5 second *tumbling* (non-overlapping sequential) window in which the total quantity for each driverId is calculated.

3. Save the query.

## Create a Query to Count the Trips

    ```
    SELECT
    COUNT(DISTINCT tripId) AS TripCount,
    System.TIMESTAMP() AS Time
    INTO [PowerBI-Dataset]
    FROM [IACTripEventHub] TIMESTAMP BY createdAt
    GROUP BY 
    TumblingWindow(second, 10)1
    ```


## Create a Query to Summarize the Trip Amounts for start and end of each 10 second tumbling (non-overlapping sequential) window.

    ```
    SELECT tripId, SUM(CAST(tripAmount AS FLOAT)) AS TenSecondFare
    INTO [PowerBI-Dataset]
    FROM [IACTripEventHub] TIMESTAMP BY createdAt
    GROUP BY tripId, TumblingWindow(second, 10)
    ```

## Create a Query to Filter the Trip Start Location

    ```
    SELECT *
    INTO [PowerBI-Dataset]
    FROM [IACTripEventHub] TIMESTAMP BY timestamp
    WHERE startLocation LIKE 'S%F'
    ```



