package net.qyjohn.DDBHotKey;

import java.io.*;
import java.util.*;
import java.nio.charset.Charset;
import com.amazonaws.*;
import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.*;
import com.amazonaws.regions.*;
import com.amazonaws.services.dynamodbv2.*;
import com.amazonaws.services.dynamodbv2.model.*;

class ShardReader extends Thread
{
        AmazonDynamoDBStreamsClient client;
	String streamArn, shardId, region;

	public ShardReader(String streamArn, String shardId, String region)
	{
                client = new AmazonDynamoDBStreamsClient();
                client.configureRegion(Regions.fromName(region));
		this.streamArn = streamArn;
		this.shardId = shardId;
	}

	public void run()
	{
		try
		{
			GetShardIteratorResult result1 = client.getShardIterator(
				new GetShardIteratorRequest()
				.withStreamArn(streamArn)
				.withShardId(shardId)
				.withShardIteratorType(ShardIteratorType.TRIM_HORIZON));
			String shardIterator = result1.getShardIterator();

			boolean hasData = true;
			while (hasData)
			{
				GetRecordsResult result2 = client.getRecords(new GetRecordsRequest().withShardIterator(shardIterator));	
				shardIterator = result2.getNextShardIterator();
				if (shardIterator == null)
				{
					hasData = false; // Shard closed.
				}

				List<Record> records = result2.getRecords();
				if (records.isEmpty())
				{
					sleep(2000);	// No records
				}
				else
				{
					for (Record record : records)
					{
						StreamRecord sRecord = record.getDynamodb();
						String data = sRecord.toString();
						System.out.println(shardId + "\t" + data);
					}
				}
			}
		}catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}

public class DDBHotKey
{
	public AmazonDynamoDBClient        ddbClient;
        public AmazonDynamoDBStreamsClient strClient;
	public String tableName, streamArn, region;

        public DDBHotKey()
        {
		try
		{
			Properties prop = new Properties();
			InputStream input = new FileInputStream("ddb.properties");
			prop.load(input);
			region    = prop.getProperty("region");
			tableName = prop.getProperty("tableName");

			ddbClient = new AmazonDynamoDBClient();
			ddbClient.configureRegion(Regions.fromName(region));
			strClient = new AmazonDynamoDBStreamsClient();
			strClient.configureRegion(Regions.fromName(region));
		}catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
        }

	public void run()
	{
		try
		{
			DescribeTableResult tblResult = ddbClient.describeTable(tableName);
			StreamSpecification stream = tblResult.getTable().getStreamSpecification();
			if (stream.isStreamEnabled())
			{
				streamArn = tblResult.getTable().getLatestStreamArn();
				DescribeStreamResult result = strClient.describeStream(
					new DescribeStreamRequest().withStreamArn(streamArn));
				StreamDescription description = result.getStreamDescription();
				List<Shard> shards = description.getShards();
				for (Shard shard : shards)
				{
					String shardId = shard.getShardId();
					new ShardReader(streamArn, shardId, region).start();
				}
			}
			else
			{
				System.out.println("Stream is not enabled for table: " + tableName);
			}
		}catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

        public static void main(String[] args)
        {
		try 
		{
			DDBHotKey demo = new DDBHotKey();
			demo.run();
		} catch (Exception e) 
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
        }
}
