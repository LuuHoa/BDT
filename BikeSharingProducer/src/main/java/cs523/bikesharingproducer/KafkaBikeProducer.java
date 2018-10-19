package cs523.bikesharingproducer;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaBikeProducer extends Thread {

    private static final String topicName = "bikeAnalytics";
    public static final String fileName = "dataset/BlackFriday.csv";
	
	/*private static  String topicName = "bikeAnalytics";
	public static  String fileName = "dataset/hour.csv";*/

    private final KafkaProducer<String, String> producer;
    private final Boolean isAsync;

    public KafkaBikeProducer(String topic, Boolean isAsync) {
        Properties props = new Properties();
        
        props.put("bootstrap.servers", "localhost:9092");  
       // props.put("client.id", "BikeProducer");
    	props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
        this.isAsync = isAsync;
    }

    public void sendMessage(String key, String value) {
        long startTime = System.currentTimeMillis();
        if (isAsync) { // Send asynchronously
            producer.send(
                    new ProducerRecord<String, String>(topicName, key),
                    (Callback) new DemoCallBack(startTime, key, value));
        } else { // Send synchronously
            producer.send(
			        new ProducerRecord<String, String>(topicName, key, value))
			       // .get()
			        ;
			System.out.println("Sent message: (" + key + ", " + value + ")");
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
    }

    public static void main(String [] args){
    /*	topicName = args[0];
    	if (topicName.equalsIgnoreCase("users"))
    	fileName = "dataset/BX-Users.csv";
    	if (topicName.equalsIgnoreCase("books"))
    	fileName = "dataset/BX-Books.csv";
    	if (topicName.equalsIgnoreCase("ratings"))
        	fileName = "dataset/BX-Book-Ratings.csv";*/
    	
    	KafkaBikeProducer producer = new KafkaBikeProducer(topicName, false);
        int lineCount = 0;
        FileInputStream fis;
        BufferedReader br = null;
        try {
            fis = new FileInputStream(fileName);
            //Construct BufferedReader from InputStreamReader
            br = new BufferedReader(new InputStreamReader(fis));

            String line = null;
            while ((line = br.readLine()) != null) {
            	System.out.println(line);
                lineCount++;
                producer.sendMessage(lineCount+"", line);
            }

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally{
            try {
                br.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }
}

class DemoCallBack implements Callback {

    private long startTime;
    private String key;
    private String message;

    public DemoCallBack(long startTime, String key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling
     * of request completion. This method will be called when the record sent to
     * the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata
     *            The metadata for the record that was sent (i.e. the partition
     *            and offset). Null if an error occurred.
     * @param exception
     *            The exception thrown during processing of this record. Null if
     *            no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println("message(" + key + ", " + message
                    + ") sent to partition(" + metadata.partition() + "), "
                    + "offset(" + metadata.offset() + ") in " + elapsedTime
                    + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}