This project uses the information in the DynamoDB Streams to analysis hot key issues with your DynamoDB table.

The AWS SDK for Java is the only dependency. 

Modify ddb.properties with the necessary information. Attribute "region" is the AWS region in which you have your DynamoDB table, "tableName" is the name of your DynamoDB table, "hashKey" is the name of the hash key of your DynamoDB table, "interval" is the sampling period in seconds (setting it to less than 5 seconds is not recommended), "topEntry" is the number of hot keys you want to display in each partition.

Below is an example of ddb.properties:

~~~~
region=ap-southeast-2
tableName=hotkey
hashKey=hash
interval=10
topEntry=10
~~~~
