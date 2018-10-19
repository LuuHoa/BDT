package Comsumer.Bike;

import java.io.IOException;
import java.io.Serializable;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.base.Optional;

import scala.Tuple2;

public class App {
	static String COLUMN_FAMILY_FIELDS = "fields";
	
    public static void main(String[] args ) {
    	//Configuration configuration = HBaseConfiguration.create();
		//try (Connection connection = ConnectionFactory.createConnection(configuration);
/*				Admin admin = connection.getAdmin();) {
			TableName tableName = TableName.valueOf("project");
			HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
			hTableDescriptor.addFamily(new HColumnDescriptor(App.COLUMN_FAMILY_FIELDS));
			if (!admin.tableExists(tableName)) {
				admin.createTable(hTableDescriptor);
				Class.forName("org.apache.hive.jdbc.HiveDriver");
				try(java.sql.Connection sqlConnection = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");) {
					Statement statement = sqlConnection.createStatement();
					String sql = "CREATE EXTERNAL TABLE project(providerId bigint, hospitalName string, address string, city string, state string, zipCode string, countyName string, phoneNumber string, sum float, count bigint)"
							+ " STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'"
							+ " WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key#b, fields:hospitalName, fields:address, fields:city, fields:state, fields:zipCode, fields:countyName, fields:phoneNumber, fields:sum#b, fields:count#b')"
							+ " TBLPROPERTIES ('hbase.table.name' = 'project')";
					statement.execute(sql);
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
			}*/
			
			//Map<String, String> kafkaParams = new HashMap<String, String>();
	    	//kafkaParams.put("bootstrap.servers", "localhost:9092");
				String brokers = args[0];

			    String groupId = args[1];

			    //String topic = args[2];
			    
			    String topic = "bikeAnalytics";
				
				Map<String, Object> kafkaParams = new HashMap<>();
				
				kafkaParams.put("bootstrap.servers", "localhost:9092");

			   // kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

			 //   kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

			//    kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

			//    kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
				
	    	Set<String> topics = new HashSet<String>();
	    	topics.add(topic);
	    	
	    	SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Project");
	    	JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
	    	JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkContext, Durations.seconds(5));
				
	    	javaStreamingContext.checkpoint(".");
	    	
	    /*	 JavaInputDStream<ConsumerRecord<String, String>> messages =  KafkaUtils.createDirectStream(
	    			javaStreamingContext,
	    			String.class,
	    			String.class,
	    			StringDecoder.class,
	    			StringDecoder.class,
	    			kafkaParams,
	    			topics);
	    	 JavaDStream<String> lines = messages.map(ConsumerRecord::value);*/

	    	 /*   JavaDStream<String> words = 
	    	    		lines.flatMap(x -> new ArrayList<String>(Arrays.asList(SPACE.split(x))));

	    	    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))

	    	        .reduceByKey((i1, i2) -> i1 + i2);

	    	    wordCounts.print();*/
	    	
	    	/*JavaHBaseContext javaHBaseContext = new JavaHBaseContext(javaSparkContext, configuration);
	    	javaHBaseContext.streamBulkPut(KafkaUtils.createDirectStream(
	    			javaStreamingContext,
	    			String.class,
	    			String.class,
	    			StringDecoder.class,
	    			StringDecoder.class,
	    			kafkaParams,
	    			topics)
	    			.map(x -> Row.parse(x._2))
	    			.mapToPair(x -> new Tuple2<Provider, Pair>(x.provider, new Pair(x.score, 1L)))
	    			.reduceByKey((x, y) -> {
	    				x.sum += y.sum;
	    				x.count += y.count;
	    				return x;
	    			})
	    			.<Pair>updateStateByKey((x, y) -> {
	    				Pair aggregatedValues = new Pair(0.0F, 0L);
	    				if(y.isPresent()) {
	    					Pair oldValues = y.get();
	    					aggregatedValues.sum += oldValues.sum;
	    					aggregatedValues.count += oldValues.count;
	    				}
	    				for(Pair v : x) {
	    					aggregatedValues.sum += v.sum;
	    					aggregatedValues.count += v.count;
	    				}
	    				return Optional.of(aggregatedValues);
	    			})
	    			.toJavaDStream(),
	                tableName,
	                x -> {
	                	return App.processLine(x._1, x._2);
	                });*/
	    	
	    	javaStreamingContext.start();
	    	javaStreamingContext.awaitTermination();
		/*} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}*/
    }
    
    static Put processLine(Provider provider, Pair pair) {
    	Put put = new Put(Bytes.toBytes(provider.providerId));
		put.addColumn(Bytes.toBytes(App.COLUMN_FAMILY_FIELDS), Bytes.toBytes("hospitalName"), Bytes.toBytes(provider.hospitalName));
		put.addColumn(Bytes.toBytes(App.COLUMN_FAMILY_FIELDS), Bytes.toBytes("address"), Bytes.toBytes(provider.address));
		put.addColumn(Bytes.toBytes(App.COLUMN_FAMILY_FIELDS), Bytes.toBytes("city"), Bytes.toBytes(provider.city));
		put.addColumn(Bytes.toBytes(App.COLUMN_FAMILY_FIELDS), Bytes.toBytes("state"), Bytes.toBytes(provider.state));
		put.addColumn(Bytes.toBytes(App.COLUMN_FAMILY_FIELDS), Bytes.toBytes("zipCode"), Bytes.toBytes(provider.zipCode));
		put.addColumn(Bytes.toBytes(App.COLUMN_FAMILY_FIELDS), Bytes.toBytes("countyName"), Bytes.toBytes(provider.countyName));
		put.addColumn(Bytes.toBytes(App.COLUMN_FAMILY_FIELDS), Bytes.toBytes("phoneNumber"), Bytes.toBytes(provider.phoneNumber));
		put.addColumn(Bytes.toBytes(App.COLUMN_FAMILY_FIELDS), Bytes.toBytes("sum"), Bytes.toBytes(pair.sum));
		put.addColumn(Bytes.toBytes(App.COLUMN_FAMILY_FIELDS), Bytes.toBytes("count"), Bytes.toBytes(pair.count));
		return put;
    }
    
    static Integer parseInt(String value) {
    	Integer parsedValue = 0;
    	try {
    		parsedValue = Integer.parseInt(value);
    	} catch (Exception e) {
    	}
    	return parsedValue;
    }
    
    static Long parseLong(String value) {
    	Long parsedValue = 0L;
    	try {
    		parsedValue = Long.parseLong(value);
    	} catch (Exception e) {
    	}
    	return parsedValue;
    }
    
    static Float parseFloat(String value) {
    	Float parsedValue = 0.0F;
    	try {
    		parsedValue = Float.parseFloat(value);
    	} catch (Exception e) {
    	}
    	return parsedValue;
    }
    
    static class Pair implements Serializable {
    	private static final long serialVersionUID = 1L;
    	
    	Pair(Float sum, Long count) {
    		this.sum = sum;
    		this.count = count;
    	}
    	
    	Float sum;
    	Long count;
    }
    
    static class Provider implements Serializable {
		private static final long serialVersionUID = 1L;
		
		Long providerId;
    	String hospitalName;
    	String address;
    	String city;
    	String state;
    	String zipCode;
    	String countyName;
    	String phoneNumber;
    	
    	@Override
    	public boolean equals(Object o) {
    		if(o == null) {
    			return false;
    		}
    		if(o == this) {
    			return true;
    		}
    		if(o instanceof Provider) {
    			Provider provider = (Provider) o;
    			if(this.providerId.equals(provider.providerId)) {
    				return true;
    			}
    		}
    		return false;
    	}
    	
    	@Override
    	public int hashCode() {
    		return this.providerId.hashCode();
    	}
	}
	
	static class Measure implements Serializable {
		private static final long serialVersionUID = 1L;
		
		String measureId;
    	String measureName;
    	
    	@Override
    	public boolean equals(Object o) {
    		if(o == null) {
    			return false;
    		}
    		if(o == this) {
    			return true;
    		}
    		if(o instanceof Measure) {
    			Measure measure = (Measure) o;
    			if(this.measureId.equalsIgnoreCase(measure.measureId)) {
    				return true;
    			}
    		}
    		return false;
    	}
    	
    	@Override
    	public int hashCode() {
    		return this.measureId.hashCode();
    	}
	}
    
    static class Row implements Serializable {
    	private static final long serialVersionUID = 1L;
    	
    	Provider provider;
    	Measure measure;
    	String compareToNational;
    	Integer denominator;
    	Float score;
    	Float lowerEstimate;
    	Float higherEstimate;
    	String footnote;
    	String measureStartDate;
    	String measureEndDate;
    	
    	static Row parse(String data) {
    		String[] values = data.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
    		Row row = new Row();
    		Provider provider = new Provider();
    		provider.providerId = App.parseLong(values[0]);
    		provider.hospitalName = values[1];
    		provider.address = values[2];
    		provider.city = values[3];
    		provider.state = values[4];
    		provider.zipCode = values[5];
    		provider.countyName = values[6];
    		provider.phoneNumber = values[7];
    		row.provider = provider;
    		Measure measure = new Measure();
    		measure.measureName = values[8];
    		measure.measureId = values[9];
    		row.measure = measure;
    		row.compareToNational = values[10];
    		row.denominator = App.parseInt(values[11]);
    		row.score = App.parseFloat(values[12]);
    		row.lowerEstimate = App.parseFloat(values[13]);
    		row.higherEstimate = App.parseFloat(values[14]);
    		row.footnote = values[15];
    		row.measureStartDate = values[16];
    		row.measureEndDate = values[17];
    		return row;
    	}
    }
}
