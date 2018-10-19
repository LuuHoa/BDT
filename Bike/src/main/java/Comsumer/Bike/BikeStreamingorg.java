package Comsumer.Bike;

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
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;

/*import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;*/
import org.apache.spark.streaming.Durations;






public final class BikeStreamingorg {

  private static final Pattern SPACE = Pattern.compile(" ");
  public static void main(String[] args) throws Exception {

/*    if (args.length < 3) {

      System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <groupId> <topics>\n" +

                         "  <brokers> is a list of one or more Kafka brokers\n" +

                         "  <groupId> is a consumer group name to consume from topics\n" +

                         "  <topics> is a list of one or more kafka topics to consume from\n\n");

      System.exit(1);

    }*/



   // StreamingExamples.setStreamingLogLevels();



/*    String brokers = args[0];

    String groupId = args[1];

    String topics = args[2];*/



    // Create context with a 2 seconds batch interval

   
    SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("JavaDirectKafkaWordCount");

    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

    System.out.println("Test");

    Set<String> topicsSet = new HashSet<>();
    topicsSet.add("bikeAnalytics");

    Map<String, String> kafkaParams = new HashMap<>();

   // kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    
    kafkaParams.put("bootstrap.servers", "localhost:9092");

    //kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

    //kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    //kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);



    // Create direct kafka stream with brokers and topics
    
  /*  KafkaUtils.createDirectStream(
			javaStreamingContext,
			String.class,
			String.class,
			StringDecoder.class,
			StringDecoder.class,
			kafkaParams,
			topics)
    */

    

	 JavaPairInputDStream<String, String> messages =  KafkaUtils.createDirectStream(
			jssc,
			String.class,
			String.class,
			StringDecoder.class,
			StringDecoder.class,
			kafkaParams,
			topicsSet);
	 
    /*JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(

        jssc,

        LocationStrategies.PreferConsistent(),

        ConsumerStrategies.Subscribe(topicsSet, kafkaParams));*/



    // Get the lines, split them into words, count the words and print

  //  JavaDStream<String> lines = messages.map(ConsumerRecord::value);
	 
	  JavaDStream<String> lines = messages.map(f -> f._2);

    JavaDStream<String> words = 
    		lines.flatMap(x -> new ArrayList<String>(Arrays.asList(SPACE.split(x))));

    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))

        .reduceByKey((i1, i2) -> i1 + i2);

    wordCounts.print();



    // Start the computation

    jssc.start();

    jssc.awaitTermination();

  }

}
