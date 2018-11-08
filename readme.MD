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
