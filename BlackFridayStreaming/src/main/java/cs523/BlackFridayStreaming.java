package cs523;

import java.io.IOException;
import java.io.Serializable;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
//import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

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
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.base.Optional;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public final class BlackFridayStreaming {

	static Logger logger = Logger.getLogger(BlackFridayStreaming.class.getName());

	private static final Pattern SPACE = Pattern.compile(" ");

	private static final String ND_TABLE_NAME = "user_sales";
	static String ONE_COLUMN_FAMILY = "fields";

	public static void main(String[] args) throws Exception {

		Configuration config = HBaseConfiguration.create();
		try (Connection connection = ConnectionFactory.createConnection(config); Admin admin = connection.getAdmin()) {
			TableName tableNameStats = TableName.valueOf(ND_TABLE_NAME);
			HTableDescriptor hTableStatsDescriptor = new HTableDescriptor(tableNameStats);
			hTableStatsDescriptor.addFamily(new HColumnDescriptor(ONE_COLUMN_FAMILY));

			if (admin.tableExists(tableNameStats)) {
				Class.forName("org.apache.hive.jdbc.HiveDriver");
				try (java.sql.Connection sqlConnection = DriverManager
						.getConnection("jdbc:hive2://localhost:10000/default", "", "");) {

					Statement statement1 = sqlConnection.createStatement();
					String sql1 = "DROP TABLE " + ND_TABLE_NAME;
					statement1.execute(sql1);
					sqlConnection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}

				admin.disableTable(tableNameStats);
				admin.deleteTable(tableNameStats);

			}

			if (!admin.tableExists(tableNameStats)) {
				admin.createTable(hTableStatsDescriptor);
				Class.forName("org.apache.hive.jdbc.HiveDriver");
				try (java.sql.Connection sqlConnection = DriverManager
						.getConnection("jdbc:hive2://localhost:10000/default", "", "");) {

					Statement statement = sqlConnection.createStatement();
					String sql = "CREATE EXTERNAL TABLE user_sales(User_ID int, Gender VARCHAR(100),Age VARCHAR(200),Occupation INT,City_Category VARCHAR(100),Stay_In_Current_City_Years VARCHAR(5),Marital_Status INT,Money_Spend DOUBLE, No_of_Purchase BIGINT)"
							+ " STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'"
							+ " WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key#b, fields:Gender, fields:Age, fields:Occupation#b, fields:City_Category, fields:Stay_In_Current_City_Years, fields:Marital_Status#b, fields:Money_Spend#b, fields:No_of_Purchase#b')"
							+ " TBLPROPERTIES ('hbase.table.name' = 'user_sales')";
					System.out.println(sql);
					statement.execute(sql);
					sqlConnection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}

			}
			;

			SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("Black Friday Streaming");
			JavaSparkContext jsc = new JavaSparkContext(sparkConf);
			JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(5));
			jssc.checkpoint(".");

			Set<String> topicsSet = new HashSet<>();
			topicsSet.add("blackFriday");

			Map<String, String> kafkaParams = new HashMap<>();

			kafkaParams.put("bootstrap.servers", "localhost:9092");
			// kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, 1);
			// kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
			// StringDeserializer.class);
			// kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
			// StringDeserializer.class);

			JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class,
					String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

			final Function3<Dimension, Optional<Tuple2<Double, Long>>, State<Tuple2<Double, Long>>, Tuple2<Dimension, Tuple2<Double, Long>>> mappingFunc =

					new Function3<Dimension, Optional<Tuple2<Double, Long>>, State<Tuple2<Double, Long>>, Tuple2<Dimension, Tuple2<Double, Long>>>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Tuple2<Dimension, Tuple2<Double, Long>> call(Dimension user,
								Optional<Tuple2<Double, Long>> v2, State<Tuple2<Double, Long>> state) throws Exception {
							// TODO Auto-generated method stub
							Tuple2<Double, Long> myValue = v2.or(new Tuple2<Double, Long>(0.0, 0L));
							Tuple2<Double, Long> stateValue = (state.exists() ? state.get()
									: new Tuple2<Double, Long>(0.0, 0L));
							Tuple2<Double, Long> sum = new Tuple2<Double, Long>(myValue._1 + stateValue._1,
									myValue._2 + stateValue._2);
							Tuple2<Dimension, Tuple2<Double, Long>> output = new Tuple2<Dimension, Tuple2<Double, Long>>(
									user, sum);
							state.update(sum);
							return output;
						}

					};

			JavaHBaseContext javaHBaseContext = new JavaHBaseContext(jsc, config);
			javaHBaseContext.streamBulkPut(messages.map(x -> SaleRecord.parse(x._2))
					.mapToPair(x -> new Tuple2<Dimension, Tuple2<Double, Long>>(x.user,
							new Tuple2<Double, Long>(x.buy.Purchase, 1L)))
					.mapWithState(StateSpec.function(mappingFunc)), tableNameStats, x -> {
						return insertDataSum(x._1, x._2);
					});

			jssc.start();
			jssc.awaitTermination();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

	}

	static Put insertData(SaleRecord row) {
		Put put = new Put(Bytes.toBytes(row.hashCode()));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("User_ID"), Bytes.toBytes(row.user.User_ID));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Product_ID"), Bytes.toBytes(row.buy.Product_ID));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Gender"), Bytes.toBytes(row.user.Gender));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Age"), Bytes.toBytes(row.user.Age));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Occupation"),
				Bytes.toBytes(row.user.Occupation));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("City_Category"),
				Bytes.toBytes(row.user.City_Category));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Stay_In_Current_City_Years"),
				Bytes.toBytes(row.user.Stay_In_Current_City_Years));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Marital_Status"),
				Bytes.toBytes(row.user.Marital_Status));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Product_Category_1"),
				Bytes.toBytes(row.buy.Product_Category_1));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Product_Category_2"),
				Bytes.toBytes(row.buy.Product_Category_2));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Product_Category_3"),
				Bytes.toBytes(row.buy.Product_Category_3));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Purchase"), Bytes.toBytes(row.buy.Purchase));
		return put;

	}

	static Put insertDataSum(Dimension row, Tuple2<Double, Long> summary) {
		Put put = new Put(Bytes.toBytes(row.User_ID));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Gender"), Bytes.toBytes(row.Gender));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Age"), Bytes.toBytes(row.Age));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Occupation"), Bytes.toBytes(row.Occupation));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("City_Category"),
				Bytes.toBytes(row.City_Category));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Stay_In_Current_City_Years"),
				Bytes.toBytes(row.Stay_In_Current_City_Years));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Marital_Status"),
				Bytes.toBytes(row.Marital_Status));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("Money_Spend"), Bytes.toBytes(summary._1));
		put.addColumn(Bytes.toBytes(ONE_COLUMN_FAMILY), Bytes.toBytes("No_of_Purchase"), Bytes.toBytes(summary._2));
		return put;

	}

	static class Dimension implements Serializable {
		private static final long serialVersionUID = 1L;

		Integer User_ID;
		String Gender;
		String Age;
		Integer Occupation;
		String City_Category;
		String Stay_In_Current_City_Years;
		Integer Marital_Status;

		@Override
		public boolean equals(Object o) {
			if (o == null) {
				return false;
			}
			if (o == this) {
				return true;
			}
			if (o instanceof Dimension) {
				Dimension provider = (Dimension) o;
				if (this.User_ID.equals(provider.User_ID)) {
					return true;
				}
			}
			return false;
		}

		@Override
		public int hashCode() {
			return this.User_ID.hashCode();
		}
	}

	static class BuyProduct implements Serializable {
		private static final long serialVersionUID = 1L;

		String Product_ID;
		Integer Product_Category_1;
		Integer Product_Category_2;
		Integer Product_Category_3;
		Double Purchase;

	}

	static class SaleRecord implements Serializable {
		private static final long serialVersionUID = 1L;
		Dimension user;
		BuyProduct buy;
		/*
		 * Integer User_ID; String Product_ID; String Gender; String Age; Integer
		 * Occupation; String City_Category; String Stay_In_Current_City_Years; Integer
		 * Marital_Status; Integer Product_Category_1; Integer Product_Category_2;
		 * Integer Product_Category_3; Double Purchase;
		 */

		static SaleRecord parse(String data) {
			String[] elements = data.split(",");
			SaleRecord row = new SaleRecord();
			Dimension tmpUser = new Dimension();
			tmpUser.User_ID = parseInt(elements[0]);
			tmpUser.Gender = elements[2];
			tmpUser.Age = elements[3];
			tmpUser.Occupation = parseInt(elements[4]);
			tmpUser.City_Category = elements[5];
			tmpUser.Stay_In_Current_City_Years = elements[6];
			tmpUser.Marital_Status = parseInt(elements[7]);
			row.user = tmpUser;

			BuyProduct tmpProd = new BuyProduct();
			tmpProd.Product_ID = elements[1];
			tmpProd.Product_Category_1 = parseInt(elements[8]);
			tmpProd.Product_Category_2 = parseInt(elements[9]);
			tmpProd.Product_Category_3 = parseInt(elements[10]);
			tmpProd.Purchase = parseDouble(elements[11]);
			row.buy = tmpProd;

			/*
			 * row.User_ID = parseInt(elements[0]); row.Product_ID = elements[1]; row.Gender
			 * = elements[2]; row.Age = elements[3]; row.Occupation = parseInt(elements[4]);
			 * row.City_Category = elements[5]; row.Stay_In_Current_City_Years =
			 * elements[6]; row.Marital_Status = parseInt(elements[7]);
			 * row.Product_Category_1 = parseInt(elements[8]); row.Product_Category_2 =
			 * parseInt(elements[9]); row.Product_Category_3 = parseInt(elements[10]);
			 * row.Purchase = parseDouble(elements[11]);
			 */

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

	}

}
