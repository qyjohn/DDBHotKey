# DynamoDB Hot Key Utility (DDBHotKey)

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

We recommend that you run the DDBHotKey utility on an EC2 instance with an IAM role. The IAM role needs to have the permissions to the access your DynamoDB table. In the easier case, you might just want dynamodb:* for quick testing.

Below are the steps to run the DDBHotKey utility on Ubuntu 16.04 / 18.04:

First of all, you need the JDK 1.8+, Maven, and git:

~~~~
cd ~
sudo apt update
sudo apt install openjdk-8-jdk maven git
git clone https://github.com/qyjohn/DDBHotKey
cd DDBHotKey
mvn package
~~~~

Now create a test table "hotkey" with a hash key (hash, String) and range key (sort, String) in DynamoDB. To make sure that you have more than 1 partitions in the test table, do not accept the default capacity settings, but rather manually assign 1500 WCU to the test table. Also, make sure that you enable DynamoDB Streams for the test table.

Modify **ddb.properties** with the correct AWS region and table name. Then start the DDBHotKey utility using the following command.

~~~~
cd ~/DDBHotKey
java -cp target/ddb-hotkey-jar-with-dependencies.jar net.qyjohn.DDBHotKey.DDBHotKey 
~~~~

In a separate SSH window, generate some workload on the test table. The parameter 20 specified means 20 different hash keys will be used in the test. You can change this to something else like 100 or 200. Please note that setting a large number will consume more WCU from your table. 

~~~~
cd ~/DDBHotKey
java -cp target/ddb-hotkey-jar-with-dependencies.jar net.qyjohn.DDBHotKey.TestDDB 20
~~~~

Below is the test output with the **topEntry** parameter set to 5:

~~~~
$ java -cp target/ddb-hotkey-jar-with-dependencies.jar net.qyjohn.DDBHotKey.DDBHotKey 
log4j:WARN No appenders could be found for logger (com.amazonaws.AmazonWebServiceClient).
log4j:WARN Please initialize the log4j system properly.
log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.

shardId-00000001541649352447-f1e80d4a
	54	{S: 923e4db9-fc24-43b9-a370-6dc329b43965,}
	52	{S: 8d37de33-daa3-4cb1-92a7-af04ea18d23f,}
	51	{S: dad8eaa5-c230-44e9-9ca2-8803e772a8c0,}
	50	{S: 109b40e2-0dd0-4df5-911e-63a0e77e7f9b,}
	50	{S: 6475e559-1a3b-45e7-ab61-665e593344e1,}


shardId-00000001541649352551-411b3cc2
	56	{S: ac52badf-618c-4c36-a110-2259b5316402,}
	54	{S: 3ca8b94c-e64a-47ae-99f9-bd8cba14ecd9,}
	52	{S: 5516a95c-415d-4805-b388-4ef0abbf1604,}
	50	{S: 650ef422-1fdf-48c9-be60-5c0a4de79e3a,}
	50	{S: cf716a9e-e582-4c9a-b8e3-49b49e5f2f34,}


shardId-00000001541649352447-f1e80d4a
	195	{S: 3d3095aa-dc8c-4b5e-b703-162c7c49a2a3,}
	191	{S: 8d37de33-daa3-4cb1-92a7-af04ea18d23f,}
	187	{S: 923e4db9-fc24-43b9-a370-6dc329b43965,}
	185	{S: 05648d36-3da8-410f-983d-5e203792dac5,}
	184	{S: dad8eaa5-c230-44e9-9ca2-8803e772a8c0,}


shardId-00000001541649352551-411b3cc2
	193	{S: f9ee9e0e-40a8-457c-a439-773dd6d8a4f0,}
	193	{S: 23761174-f690-4f9a-8f9f-9aa00e0ef445,}
	192	{S: 026cda55-f7eb-4dad-814b-e265bdd2e777,}
	192	{S: f039ff6e-e0c9-4b12-aba6-aa5190cf60fc,}
	189	{S: ac52badf-618c-4c36-a110-2259b5316402,}
~~~~

**(3) Others**

DynamoDB Streams only capture write activities in your DynamoDB table. As such, the DDBHotKey utility can not be used to analyze read-intensive workload. 


