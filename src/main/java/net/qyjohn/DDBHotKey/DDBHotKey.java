package net.qyjohn.DDBHotKey;

import java.io.*;
import java.util.*;
import java.util.stream.*;
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
	String streamArn, shardId, hashKey, region;
	int interval, topEntry;

	/**
	 *
	 * Constructor. Each instance of the ShardReader is responsible for handling the data from one shard. 
	 *
	 */

	public ShardReader(String streamArn, String shardId, String hashKey, String region, int interval, int topEntry)
	{
                client = new AmazonDynamoDBStreamsClient();
                client.configureRegion(Regions.fromName(region));
		this.streamArn = streamArn;
		this.shardId   = shardId;
		this.hashKey   = hashKey;
		this.interval  = interval;
		this.topEntry  = topEntry;
	}

	/**
	 *
	 * The run() method performs the actual data collection and data analysis work. 
	 *
	 */

	public void run()
	{
		try
		{
			GetShardIteratorResult result1 = client.getShardIterator(
				new GetShardIteratorRequest()
				.withStreamArn(streamArn)
				.withShardId(shardId)
				.withShardIteratorType(ShardIteratorType.LATEST));
			String shardIterator = result1.getShardIterator();

			while (true)
			{
				long endTime = System.currentTimeMillis() + 1000*interval;
				HashMap<AttributeValue, Integer> map = new HashMap<AttributeValue, Integer>();
				int total = 0;

				// Here we start a new sampling cycle. Start collecting data until the end of the sampling cycle. 
				while (System.currentTimeMillis() < endTime)
				{
					GetRecordsResult result2 = client.getRecords(new GetRecordsRequest().withShardIterator(shardIterator));	
					shardIterator = result2.getNextShardIterator();
					List<Record> records = result2.getRecords();
					if (records.isEmpty())
					{
						sleep(1000);	// No records, sleep 1 second
					}
					else
					{
						for (Record record : records)
						{
							// Try to get the New Image first. If New Image is not available, get the Keys
							AttributeValue key;
							key = record.getDynamodb().getNewImage().get(hashKey);
							if (key == null)
							{
								key = record.getDynamodb().getKeys().get(hashKey);
							}
							if (map.containsKey(key))
							{
								map.put(key, new Integer(map.get(key).intValue() + 1));
							}
							else
							{
								map.put(key, new Integer(1));
							}
							// Count the total number of write requests in the sampling period
							total++;
						}
					}
				}

				// Performing statistics.
				if (!map.isEmpty())
				{
					Map<AttributeValue, Integer> sortedMap = 
						map.entrySet().stream()
						.sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
						.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
						(e1, e2) -> e1, LinkedHashMap::new));
					
					String output = "\n" + shardId + " Total: " + total;
					Iterator it = sortedMap.entrySet().iterator();

					int count = 0;
					while (it.hasNext() && (count < topEntry)) 
					{
						Map.Entry<AttributeValue, Integer> pair = (Map.Entry)it.next();
						String percent = String.format("%.1f", 100 * ((float) pair.getValue().intValue() / (float) total));
						output = output + "\n\t" + pair.getValue() + "\t" + percent + " %\t" + pair.getKey();
						count++;
					}
					output = output + "\n";
					System.out.println(output);
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
	public String tableName, streamArn, hashKey, region;
	public int interval, topEntry;

	/**
	 *
	 * Constructor. Loads configuration from ddb.properties. Also create the 
	 * AmazonDynamoDBClient and AmazonDynamoDBStreamsClient.
	 *
	 */

        public DDBHotKey()
        {
		try
		{
			Properties prop = new Properties();
			InputStream input = new FileInputStream("ddb.properties");
			prop.load(input);
			region    = prop.getProperty("region");
			tableName = prop.getProperty("tableName");
			hashKey   = prop.getProperty("hashKey");
			interval  = Integer.parseInt(prop.getProperty("interval"));
			topEntry  = Integer.parseInt(prop.getProperty("topEntry"));

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

	/**
	 *
	 * The run() method checks if the DynamoDB table has enabled streams. If yes, 
	 * get teh number of shards in the Stream. For each shard, create an instance of 
	 * ShardReader to handle the incoming data. 
	 *
	 */

	public void run()
	{
		try
		{
			DescribeTableResult tblResult = ddbClient.describeTable(tableName);
			streamArn = tblResult.getTable().getLatestStreamArn();
			if (streamArn != null)
			{
				streamArn = tblResult.getTable().getLatestStreamArn();
				DescribeStreamResult result = strClient.describeStream(
					new DescribeStreamRequest().withStreamArn(streamArn));
				StreamDescription description = result.getStreamDescription();
				List<Shard> shards = description.getShards();
				for (Shard shard : shards)
				{
					String shardId = shard.getShardId();
					new ShardReader(streamArn, shardId, hashKey, region, interval, topEntry).start();
				}
			}
			else
			{
				System.out.println("\n\n DynamoDB Stream is not enabled for table: " + tableName + "\n\n");
			}
		}catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 *
	 * Test method.
	 *
	 */

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
