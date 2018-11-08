This project uses the information in DynamoDB Streams to analysis hot key issues with your DynamoDB table when you have a **write-intensive workload**.

The AWS SDK for Java is the only dependency. 

Modify **ddb.properties** with the necessary information. Attribute **region** is the AWS region in which you have your DynamoDB table, **tableName** is the name of your DynamoDB table, **hashKey** is the name of the hash key of your DynamoDB table, **interval** is the sampling period in seconds (setting it to less than 5 seconds is not recommended), **topEntry** is the number of hot keys you want to display in each partition.

Below is an example of **ddb.properties**:

~~~~
region=ap-southeast-2
tableName=hotkey
hashKey=hash
interval=10
topEntry=10
~~~~

**(1) How does it work?**

When you enable [DynamoDB Streams](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html) for your DynamoDB table, DynamoDB Streams writes a record with the primary key attribute(s) of the items that are modified in near real time. The DDBHotKey utility reads from the stream and performs simple statistics, as below:

- For each partition in the DynamoDB table, there is a corresponding shard in the stream. 

- For each shard in the stream, the DDBHotKey utility launches a separate thread to perform the statics.

- For each sampling interval (defined by the **interval** parameter), the DDBHotKey utility prints out the top N (defined by the **topEntry** parameter) hot keys in each partition (shard). If there is no write activity in a partition, then there is no data in the corresponding shard. 

**(2) How to use it?**


