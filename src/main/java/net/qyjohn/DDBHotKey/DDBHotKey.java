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
	int interval;

	public ShardReader(String streamArn, String shardId, String hashKey, String region, int interval)
	{
                client = new AmazonDynamoDBStreamsClient();
                client.configureRegion(Regions.fromName(region));
		this.streamArn = streamArn;
		this.shardId   = shardId;
		this.hashKey   = hashKey;
		this.interval  = interval;
	}

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
							AttributeValue key = record.getDynamodb().getKeys().get(hashKey);
							if (map.containsKey(key))
							{
								map.put(key, new Integer(map.get(key).intValue() + 1));
							}
							else
							{
								map.put(key, new Integer(1));
							}
						}
					}
				}

				if (!map.isEmpty())
				{
					Map<AttributeValue, Integer> sortedMap = 
						map.entrySet().stream()
						.sorted(Map.Entry.comparingByValue())
						.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
						(e1, e2) -> e1, LinkedHashMap::new));

					System.out.println("\n" + shardId);
					Iterator it = sortedMap.entrySet().iterator();
					while (it.hasNext()) 
					{
						Map.Entry pair = (Map.Entry)it.next();
						System.out.println("\t" + pair.getValue() + "\t" + pair.getKey());
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
	public String tableName, streamArn, hashKey, region;
	public int interval;

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
					new ShardReader(streamArn, shardId, hashKey, region, interval).start();
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
