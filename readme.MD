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

Below is the test output with the **topEntry** parameter set to 5. In the output you first see the shard id (with the total number of write request to that shard), then the number of requests (and the percentage with the base being the total number of write request to that shard) for the top N hash keys in each shard, in descending order.

~~~~
$ java -cp target/ddb-hotkey-jar-with-dependencies.jar net.qyjohn.DDBHotKey.DDBHotKey 
log4j:WARN No appenders could be found for logger (com.amazonaws.AmazonWebServiceClient).
log4j:WARN Please initialize the log4j system properly.
log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.

shardId-00000001541748842285-2f4e3c5c Total: 2600
	464	17.8 %	{S: 4f7062e6-32e5-4c77-b156-5f67ef2f8864,}
	433	16.7 %	{S: 7f8c5a67-56bd-4611-903a-c116c512c2d6,}
	290	11.2 %	{S: c0420504-4596-4862-9712-504f8a154479,}
	262	10.1 %	{S: 7da56ddf-a5e7-43e2-847c-0bc4e1802502,}
	229	8.8 %	{S: 48bd7d4f-cc23-4d3b-9e35-eb9a8253261e,}


shardId-00000001541748842182-d55ec92e Total: 2423
	493	20.3 %	{S: ffe90d30-9328-495b-9f48-f8913de35ca1,}
	482	19.9 %	{S: 4f4dcc6a-fde2-4c27-95ec-8c339c862f61,}
	300	12.4 %	{S: 1d0887f1-5aca-4753-94d2-922484d7ebdb,}
	276	11.4 %	{S: 1b02cf57-4da0-40ec-aba3-fd758316ae0b,}
	223	9.2 %	{S: ba05c4d2-d2ff-4584-b18e-e331ec6d29b6,}


shardId-00000001541748842285-2f4e3c5c Total: 2831
	513	18.1 %	{S: 4f7062e6-32e5-4c77-b156-5f67ef2f8864,}
	468	16.5 %	{S: 7f8c5a67-56bd-4611-903a-c116c512c2d6,}
	334	11.8 %	{S: c0420504-4596-4862-9712-504f8a154479,}
	281	9.9 %	{S: 7da56ddf-a5e7-43e2-847c-0bc4e1802502,}
	254	9.0 %	{S: 48bd7d4f-cc23-4d3b-9e35-eb9a8253261e,}


shardId-00000001541748842182-d55ec92e Total: 2614
	530	20.3 %	{S: 4f4dcc6a-fde2-4c27-95ec-8c339c862f61,}
	497	19.0 %	{S: ffe90d30-9328-495b-9f48-f8913de35ca1,}
	327	12.5 %	{S: 1d0887f1-5aca-4753-94d2-922484d7ebdb,}
	304	11.6 %	{S: 1b02cf57-4da0-40ec-aba3-fd758316ae0b,}
	247	9.4 %	{S: ba05c4d2-d2ff-4584-b18e-e331ec6d29b6,}
~~~~

**(3) Others**

DynamoDB Streams only capture write activities in your DynamoDB table. As such, the DDBHotKey utility can not be used to analyze read-intensive workload. 

Since we use a dedicate thread to handle the activities in each shard (DynamoDB partition), the computing cost is proportional to (a) the number of shards in the stream, and (b) the level of activities in each shard. In our tests on AWS EC2, a single vCPU can easily handle 2~4 threads easily. For example, if you have 16 shards in the stream, an EC2 instance with 4 vCPU cores should be able to handle the workload gracefully. If you have an EC2 instance with 32 vCPU cores, you can probably handle around 4 x 32 = 128 shards. 

Memory consumption is proportion to the amount of write activities during one sampling cycle. Think about you have a DynamoDB table with 100,000 WCU and you are fully utilizing all of it, you are able to write to your table with a 100,000 x 1 KB = 100 MB/s throughput at maximum. If you set the sampling period (**interval**) to 10 seconds, then you will need 10 x 100 = 1000 MB memory to store the data retrieved from the stream. In order to perform the statistics on the data, we need approximately 4 times the memory, which is around 4000 MB. An EC2 instance with 32 vCPU cores would have over 60 GB memory, which is more than sufficient.

This utility is intended for trouble shooting use only. You can start the utility to observe the activities on your DynamoDB table for a short period of time, then stop it when you are no longer looking. It is not designed as a long running process. 