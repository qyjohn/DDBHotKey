# DynamoDB Hot Key Utility (DDBHotKey)

This project uses the information in DynamoDB Streams to analyze hot key issues with your DynamoDB table when you have a **write-intensive workload**.

The AWS SDK for Java is the only software dependency. There is no other AWS service involved apart from DynamoDB Streams. You do not need to modify/interupt your existing DynamoDB application to use the this utility. The utility performs the said analysis in a non-intrusive way, with near real time response time. 

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

We recommend that you run the DDBHotKey utility on an EC2 instance with an IAM role. The IAM role needs to have the permissions to perform **dynamodb:DescribeTable**, **dynamodb:DescribeStream**, **dynamodb:GetShardIterator**, **dynamodb:GetRecords** on your DynamoDB table. In the following example, we add **dynamodb:PutItem** to the IAM policy because we want to write test data to the test DynamoDB table. If you are not planning to write to your DynamoDB table from the same EC2 instance, you should remove **dynamodb:PutItem** from your IAM policy. 

~~~~
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "dynamodb:DescribeStream",
                "dynamodb:GetShardIterator",
                "dynamodb:GetRecords"
            ],
            "Resource": "*"
        },
        {
            "Sid": "VisualEditor1",
            "Effect": "Allow",
            "Action": [
                "dynamodb:PutItem",
                "dynamodb:DescribeTable"
            ],
            "Resource": "*"
        }
    ]
}
~~~~

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

Below is the test output with the **topEntry** parameter set to 5. In the output you first see the shard id, then the number of requests for the top N hash keys in each shard, in descending order.

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

Since we use a dedicate thread to handle the activities in each shard (DynamoDB partition), the computing cost is proportional to (a) the number of shards in the stream, and (b) the level of activities in each shard. In our tests on AWS EC2, a single vCPU can easily handle 2~4 threads easily. For example, if you have 16 shards in the stream, an EC2 instance with 4 vCPU cores should be able to handle the workload gracefully. If you have an EC2 instance with 32 vCPU cores, you can probably handle around 4 x 32 = 128 shards. 

Memory consumption is proportion to the amount of write activities during one sampling cycle. Think about you have a DynamoDB table with 100,000 WCU and you are fully utilizing all of it, you are able to write to your table with a 100,000 x 1 KB = 100 MB/s throughput at maximum. If you set the sampling period (**interval**) to 10 seconds, then you will need 10 x 100 = 1000 MB memory to store the data retrieved from the stream. In order to perform the statistics on the data, we need approximately 4 times the memory, which is around 4000 MB. An EC2 instance with 32 vCPU cores would have over 60 GB memory, which is more than sufficient.