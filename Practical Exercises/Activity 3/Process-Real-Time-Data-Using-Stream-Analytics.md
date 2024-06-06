# Process the Real-Time Data Using Azure Stream Analytics

## View The Streaming Data Source
Before creating an Azure Stream Analytics job to process real-time data, let's take a look at the **Imaginary Airport Cab (IAC)** JSON source data.

https://github.com/PacktPublishing/DP-203-Azure-Data-Engineer-Associate-Certification-Guide-Second-Edition/blob/main/Practical%20Exercises/Activity%203/Data/IAC-TripData.json

## Create a query

Once you defined an input and an output for your Azure Stream Analytics job, you can use a query to select, filter, and aggregate data from the input and send the results to the output.

1. View the **Query** page for the Stream Analytics job. Then wait a few moments until the input preview is displayed (based on the trip events previously captured in the event hub).

2. Observe that the input data includes the **tripId**, **driverId**, **customerId**, **tripDate**, **startLocation**, **endLocation**, **driverName**, **customerName** and **tripAmount** fields in the messages submitted by EventHub , as well as additional Event Hubs fields - including the **EventProcessedUtcTime** field that indicates when the event was added to the event hub.

3. Use the query as follows: 

    ```
    SELECT
        DateAdd(second,-10,System.TimeStamp) AS StartTime,
        System.TimeStamp AS EndTime,
        driverId,
        SUM(tripAmount) AS TripAmount
    INTO
        [IACTripStorage] # Change this Output with your Storage Account
    FROM
        [IACTripEventHub] TIMESTAMP BY EventProcessedUtcTime  # Change this Input with your EventHub Name
    GROUP BY driverId, TumblingWindow(second, 10)
    HAVING COUNT(*) > 1
    ```

    Observe that this query uses the **System.Timestamp** (based on the **EventProcessedUtcTime** field) to define the start and end of each 10 second *tumbling* (non-overlapping sequential) window in which the total quantity for each driverID is calculated.

4. Use the **Test query** button to validate the query, and ensure that the **test Results** status indicates **Success**.

5. Save the query.

## Run the streaming job

OK, now you're ready to run the job and process stream IAC-TripData.

1. View the **Overview** page for the tream Analytics job, and on the **Properties** tab review the **Inputs**, **Query**, **Outputs**, and **Functions** for the job. 

2. Select the **Start** button, and start the streaming job now. Wait until you are notified that the streaming job started successfully.

3. While the stream events are running in the Azure portal, return to the storage account **Azure ADLS Gen2**.

4. Select the **Container**, you configured in the storage account to store the output data stream.

5. Open the **container**, navigate through the folder hierarchy, . Note the file that has been created, which should have a name similar to **0_xxxxxxxxxxxxxxxx.json**.


6. On the **...** menu for the file (to the right of the file details), select **View/edit**, and review the contents of the file; which should consist of a JSON record for each 10 second period, showing the number of trips processed for each driverId, like this:

```
{"tripId":100,"driverId":200,"customerId":300,"tripDate":"01/01/2022","startLocation":"New York","endLocation":"New Jersey","driverName":"John Smith","customerName":"Alice Lee","tripAmount":63.77,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":101,"driverId":201,"customerId":301,"tripDate":"01/01/2022","startLocation":"Miami","endLocation":"Dallas","driverName":"Emily Johnson","customerName":"Tom Clark","tripAmount":42.67,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":102,"driverId":202,"customerId":302,"tripDate":"02/01/2022","startLocation":"Phoenix","endLocation":"Tempe","driverName":"Michael Williams","customerName":"Lucy Lewis","tripAmount":48.51,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":103,"driverId":203,"customerId":303,"tripDate":"04/02/2022","startLocation":"LA","endLocation":"San Jose","driverName":"Jessica Brown","customerName":"Sophia Walker","tripAmount":65.92,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":104,"driverId":204,"customerId":304,"tripDate":"05/02/2022","startLocation":"Seattle","endLocation":"Redmond","driverName":"David Jones","customerName":"Henry Hall","tripAmount":49.75,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":105,"driverId":205,"customerId":305,"tripDate":"01/03/2022","startLocation":"Atlanta","endLocation":"Chicago","driverName":"Chris Davis","customerName":"Liam Allen","tripAmount":51.66,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":106,"driverId":217,"customerId":304,"tripDate":"05/07/2022","startLocation":"Las Vegas","endLocation":"Miami","driverName":"Daniel Lopez","customerName":"Henry Hall","tripAmount":72.14,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":107,"driverId":222,"customerId":336,"tripDate":"30/06/2022","startLocation":"Miami","endLocation":"Phoenix","driverName":"Sarah Wilson","customerName":"Elijah Mitchell","tripAmount":78.62,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":108,"driverId":204,"customerId":330,"tripDate":"10/06/2022","startLocation":"Denver","endLocation":"New York","driverName":"David Jones","customerName":"Benjamin Nelson","tripAmount":39.27,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":109,"driverId":246,"customerId":320,"tripDate":"15/07/2022","startLocation":"Atlanta","endLocation":"LA","driverName":"Linda White","customerName":"Ethan Scott","tripAmount":99.17,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":110,"driverId":230,"customerId":313,"tripDate":"12/11/2022","startLocation":"LA","endLocation":"Miami","driverName":"Paul Taylor","customerName":"Jacob Wright","tripAmount":77.12,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":111,"driverId":235,"customerId":321,"tripDate":"31/08/2022","startLocation":"Phoenix","endLocation":"Phoenix","driverName":"Jennifer Moore","customerName":"Aiden Green","tripAmount":71.08,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":112,"driverId":240,"customerId":350,"tripDate":"27/12/2022","startLocation":"San Francisco","endLocation":"Atlanta","driverName":"Samantha Thomas","customerName":"Daniel Campbell","tripAmount":56.12,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":113,"driverId":225,"customerId":313,"tripDate":"15/12/2022","startLocation":"Boston","endLocation":"Las Vegas","driverName":"Mark Anderson","customerName":"Jacob Wright","tripAmount":36.49,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":114,"driverId":238,"customerId":346,"tripDate":"04/07/2022","startLocation":"Phoenix","endLocation":"Houston","driverName":"Andrew Jackson","customerName":"William Roberts","tripAmount":49.98,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":115,"driverId":200,"customerId":341,"tripDate":"26/06/2022","startLocation":"Las Vegas","endLocation":"LA","driverName":"John Smith","customerName":"Alexander Perez","tripAmount":56.27,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":116,"driverId":240,"customerId":306,"tripDate":"12/11/2022","startLocation":"New York","endLocation":"San Francisco","driverName":"Samantha Thomas","customerName":"Oliver Young","tripAmount":59.0,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":117,"driverId":209,"customerId":328,"tripDate":"30/07/2022","startLocation":"Las Vegas","endLocation":"New York","driverName":"Robert Miller","customerName":"Lucas Adams","tripAmount":81.86,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":118,"driverId":212,"customerId":334,"tripDate":"15/12/2022","startLocation":"Atlanta","endLocation":"Miami","driverName":"Mary Martinez","customerName":"Noah Carter","tripAmount":75.53,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":119,"driverId":200,"customerId":347,"tripDate":"10/06/2022","startLocation":"San Francisco","endLocation":"New York","driverName":"John Smith","customerName":"Sebastian Turner","tripAmount":23.51,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":120,"driverId":216,"customerId":330,"tripDate":"21/01/2022","startLocation":"Phoenix","endLocation":"Miami","driverName":"James Hernandez","customerName":"Benjamin Nelson","tripAmount":41.69,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":121,"driverId":206,"customerId":302,"tripDate":"03/08/2022","startLocation":"Denver","endLocation":"Phoenix","driverName":"Patricia Garcia","customerName":"Lucy Lewis","tripAmount":29.79,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":122,"driverId":201,"customerId":311,"tripDate":"20/05/2022","startLocation":"San Francisco","endLocation":"Las Vegas","driverName":"Emily Johnson","customerName":"Mason King","tripAmount":94.12,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":123,"driverId":200,"customerId":322,"tripDate":"31/08/2022","startLocation":"Seattle","endLocation":"New York","driverName":"John Smith","customerName":"Michael Wright","tripAmount":55.86,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":124,"driverId":234,"customerId":314,"tripDate":"05/07/2022","startLocation":"Houston","endLocation":"Atlanta","driverName":"Charles Martinez","customerName":"Lucas Phillips","tripAmount":41.17,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":125,"driverId":210,"customerId":304,"tripDate":"10/12/2022","startLocation":"Miami","endLocation":"Phoenix","driverName":"Joseph Rodriguez","customerName":"Henry Hall","tripAmount":84.71,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":126,"driverId":239,"customerId":342,"tripDate":"09/10/2022","startLocation":"Dallas","endLocation":"Miami","driverName":"Nancy Lee","customerName":"Daniel Sanchez","tripAmount":57.84,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":127,"driverId":224,"customerId":322,"tripDate":"27/12/2022","startLocation":"New York","endLocation":"Miami","driverName":"Richard Lewis","customerName":"Michael Wright","tripAmount":28.17,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":128,"driverId":221,"customerId":314,"tripDate":"18/06/2022","startLocation":"Phoenix","endLocation":"Houston","driverName":"Karen Walker","customerName":"Lucas Phillips","tripAmount":69.93,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":129,"driverId":231,"customerId":311,"tripDate":"24/01/2022","startLocation":"San Francisco","endLocation":"New York","driverName":"Sarah Hall","customerName":"Mason King","tripAmount":39.97,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":130,"driverId":222,"customerId":319,"tripDate":"28/09/2022","startLocation":"Houston","endLocation":"Las Vegas","driverName":"Sarah Wilson","customerName":"Mason King","tripAmount":83.12,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":131,"driverId":200,"customerId":348,"tripDate":"12/02/2022","startLocation":"Phoenix","endLocation":"Seattle","driverName":"John Smith","customerName":"Anthony Evans","tripAmount":79.51,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":132,"driverId":206,"customerId":318,"tripDate":"21/10/2022","startLocation":"Seattle","endLocation":"San Francisco","driverName":"Patricia Garcia","customerName":"Christopher Scott","tripAmount":24.99,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":133,"driverId":203,"customerId":327,"tripDate":"16/08/2022","startLocation":"Phoenix","endLocation":"Miami","driverName":"Jessica Brown","customerName":"Nathan Campbell","tripAmount":37.87,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":134,"driverId":207,"customerId":316,"tripDate":"10/11/2022","startLocation":"San Francisco","endLocation":"Miami","driverName":"Linda White","customerName":"James Taylor","tripAmount":39.28,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
{"tripId":135,"driverId":243,"customerId":307,"tripDate":"18/10/2022","startLocation":"Phoenix","endLocation":"Miami","driverName":"Angela Harris","customerName":"Gabriel White","tripAmount":29.65,"EventProcessedUtcTime":"2024-06-06T12:47:52.4369238Z","PartitionId":0,"EventEnqueuedUtcTime":"2024-06-06T12:47:52.1740000Z"}
```

7. Refresh the file to see the full set of results that were produced.
8. Return to the Stream Analytics Job; At the top of the Stream Analytics job page, use the **Stop** button to stop the job.