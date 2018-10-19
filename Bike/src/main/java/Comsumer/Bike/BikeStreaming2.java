package Comsumer.Bike;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import scala.Tuple2;




import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;



import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.base.Optional;

import kafka.serializer.StringDecoder;

/*import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;*/
import org.apache.spark.streaming.Durations;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;



public final class BikeStreaming2 {
	
	 static Logger logger = Logger.getLogger(BikeStreaming2.class.getName());


  private static final Pattern SPACE = Pattern.compile(" ");
  
  private static final String THE_TABLE_NAME = "black_friday";
  static String ONE_COLUMN_FAMILY = "fields";
  
  public static void main(String[] args) throws Exception {


	  logger.info("Entering application.");

	     logger.info("Exiting application.");

	  
	       Configuration config = HBaseConfiguration.create();
	          try (Connection connection = ConnectionFactory.createConnection(config);
			Admin admin = connection.getAdmin()) {
				TableName tableName = TableName.valueOf(THE_TABLE_NAME);
				HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
				hTableDescriptor.addFamily(new HColumnDescriptor(ONE_COLUMN_FAMILY));
				System.out.println("Create table");
				
				if (admin.tableExists(tableName))
				{
					Class.forName("org.apache.hive.jdbc.HiveDriver");
					try(java.sql.Connection sqlConnection = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");) {
						
						Statement statement1 = sqlConnection.createStatement();
						String sql1 = "DROP TABLE "+THE_TABLE_NAME;
						statement1.execute(sql1);
						sqlConnection.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
					
					admin.disableTable(tableName);
					admin.deleteTable(tableName);
									
					
				}
				
				if (!admin.tableExists(tableName)) {
					admin.createTable(hTableDescriptor);
					Class.forName("org.apache.hive.jdbc.HiveDriver");
					try(java.sql.Connection sqlConnection = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");) {
						
						Statement statement = sqlConnection.createStatement();
						String sql = "CREATE EXTERNAL TABLE black_friday(line int, User_ID int, Product_ID VARCHAR(200),Gender VARCHAR(100),Age VARCHAR(200),Occupation INT,City_Category VARCHAR(100),Stay_In_Current_City_Years VARCHAR(5),Marital_Status INT,Product_Category_1 INT,Product_Category_2 INT,Product_Category_3 INT,Purchase DOUBLE)"
								+ " STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'"
								+ " WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key#b, fields:User_ID#b, fields:Product_ID, fields:Gender, fields:Age, fields:Occupation#b, fields:City_Category, fields:Stay_In_Current_City_Years, fields:Marital_Status#b, fields:Product_Category_1#b, fields:Product_Category_2#b, fields:Product_Category_3#b,fields:Purchase#b')"
								+ " TBLPROPERTIES ('hbase.table.name' = 'black_friday')";
						System.out.println(sql);
						statement.execute(sql);
						//sqlConnection.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
					
				};
			
	  
	  
    SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("JavaDirectKafkaWordCount");

    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(5));
    jssc.checkpoint(".");
	
   
    
  
    Set<String> topicsSet = new HashSet<>();
    topicsSet.add("bikeAnalytics");

    Map<String, String> kafkaParams = new HashMap<>();

   // kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    
    kafkaParams.put("bootstrap.servers", "localhost:9092");

    //kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

    //kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    //kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);



    // Create direct kafka stream with brokers and topics
    

/*	 JavaPairInputDStream<String, String> messages =  KafkaUtils.createDirectStream(
			jssc,
			String.class,
			String.class,
			StringDecoder.class,
			StringDecoder.class,
			kafkaParams,
			topicsSet);
	 
	 messages.print();

	 JavaDStream<Row> lines = messages.map(f -> f._2).map(x -> { System.out.println(x); return Row.parse(x);});
	lines.print();*/
	    /*JavaDStream<String> words = 
	    		lines.flatMap(x -> new ArrayList<String>(Arrays.asList(x.split(","))));

	    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))

	        .reduceByKey((i1, i2) -> i1 + i2);

	    wordCounts.print();*/
	    
/*
	 JavaPairInputDStream<String, String> messages =  KafkaUtils.createDirectStream(
			jssc,
			String.class,
			String.class,
			StringDecoder.class,
			StringDecoder.class,
			kafkaParams,
			topicsSet);*/
	 
    

	JavaHBaseContext javaHBaseContext = new JavaHBaseContext(jsc, config);
 	javaHBaseContext.streamBulkPut(KafkaUtils.createDirectStream(
 			jssc,
 			String.class,
 			String.class,
 			StringDecoder.class,
			StringDecoder.class,
 			kafkaParams,
 			topicsSet)
 			.map(x -> {  logger.info("Exiting application."); return Row.parse(x._2);}),
 			tableName,
             x -> {
            	 System.out.println(x);
            	  logger.info("Exiting application...");
             	return insertData(x);
             });
 	

    // Start the computation

    jssc.start();

    jssc.awaitTermination();
	          } catch (IOException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
 	

    // Get the lines, split them into words, count the words and print

  //  JavaDStream<String> lines = messages.map(ConsumerRecord::value);
/*	 
	JavaDStream<String> lines = messages.map(f -> f._2);

    JavaDStream<String> words = 
    		lines.flatMap(x -> new ArrayList<String>(Arrays.asList(SPACE.split(x))));

    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))

        .reduceByKey((i1, i2) -> i1 + i2);

    wordCounts.print();
*/



  }
  
  
  static Put insertData(Row row) {
  	    Put put = new Put(Bytes.toBytes(row.hashCode()));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("User_ID"), Bytes.toBytes(row.User_ID));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Product_ID"), Bytes.toBytes(row.Product_ID));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Gender"), Bytes.toBytes(row.Gender));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Age"), Bytes.toBytes(row.Age));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Occupation"), Bytes.toBytes(row.Occupation));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("City_Category"), Bytes.toBytes(row.City_Category));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Stay_In_Current_City_Years"), Bytes.toBytes(row.Stay_In_Current_City_Years));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Marital_Status"), Bytes.toBytes(row.Marital_Status));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Product_Category_1"), Bytes.toBytes(row.Product_Category_1));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Product_Category_2"), Bytes.toBytes(row.Product_Category_2));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Product_Category_3"), Bytes.toBytes(row.Product_Category_3));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Purchase"), Bytes.toBytes(row.Purchase));
		return put;
	  
	 /* Put put = new Put(Bytes.toBytes(row.hashCode()));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("User_ID"), Bytes.toBytes(1));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Product_ID"), Bytes.toBytes(1));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Gender"), Bytes.toBytes(1));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Age"), Bytes.toBytes(1));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Occupation"), Bytes.toBytes(1));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("City_Category"), Bytes.toBytes(1));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Stay_In_Current_City_Years"), Bytes.toBytes(1));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Marital_Status"), Bytes.toBytes(1));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Product_Category_1"), Bytes.toBytes(1));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Product_Category_2"), Bytes.toBytes(1));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Product_Category_3"), Bytes.toBytes(1));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Purchase"), Bytes.toBytes(1));
		return put;*/
		
  }
  
  static class Row implements Serializable {
  	private static final long serialVersionUID = 1L;
  	
  	Integer User_ID;
  	String Product_ID;
  	String Gender;
  	String Age;
  	Integer Occupation;
  	String City_Category;
  	String Stay_In_Current_City_Years;
  	Integer Marital_Status;
  	Integer Product_Category_1;
  	Integer Product_Category_2;
  	Integer Product_Category_3;
  	Double Purchase;
  	
  	
  	static Row parse(String data) {
  		System.out.println(data);
  		String[] elements = data.split(",");
  		Row row = new Row();
  		for( String e : elements)
		{
		System.out.println("my element isssssssssssssssssssssssss "+e);
		}
  		row.User_ID = parseInt(elements[0]);
  		row.Product_ID	= elements[1];
  		row.Gender	= elements[2];
  		row.Age	= elements[3];
  		row.Occupation	= parseInt(elements[4]);
  		row.City_Category	= elements[5];
  		row.Stay_In_Current_City_Years	= elements[6];
  		row.Marital_Status	= parseInt(elements[7]);
  		row.Product_Category_1	= parseInt(elements[8]);
  		row.Product_Category_2	= parseInt(elements[9]);
  		row.Product_Category_3	= parseInt(elements[10]);
  		row.Purchase = parseDouble(elements[11]);

  		return row;
  	}


  	static Integer parseInt(String value) {
    	Integer parsedValue = 0;
    	try {
    		parsedValue = Integer.parseInt(value);
    	} catch (Exception e) {
    	}
    	return parsedValue;
    }
    
 
    
    static Double parseDouble(String value) {
    	Double parsedValue = 0.0;
    	try {
    		parsedValue = Double.parseDouble(value);
    	} catch (Exception e) {
    	}
    	return parsedValue;
    }
    
	@Override
	public String toString() {
		return "Row [User_ID=" + User_ID + ", Product_ID=" + Product_ID + ", Gender=" + Gender + ", Age=" + Age
				+ ", Occupation=" + Occupation + ", City_Category=" + City_Category + ", Stay_In_Current_City_Years="
				+ Stay_In_Current_City_Years + ", Marital_Status=" + Marital_Status + ", Product_Category_1="
				+ Product_Category_1 + ", Product_Category_2=" + Product_Category_2 + ", Product_Category_3="
				+ Product_Category_3 + ", Purchase=" + Purchase + "]";
	}
  	
  }

}
