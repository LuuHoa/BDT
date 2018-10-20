package cs523;

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

public class BlackFridayProducer extends Thread {

	private static final String topicName = "blackFriday";
	public static final String fileName = "dataset/BlackFriday.csv";

	private final KafkaProducer<String, String> producer;
	
	public BlackFridayProducer(String topic) {
		Properties props = new Properties();

		props.put("bootstrap.servers", "localhost:9092");
		// props.put("client.id", "BikeProducer");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<String, String>(props);
	}

	public void sendMessage(String key, String value) {
		long startTime = System.currentTimeMillis();
		producer.send(new ProducerRecord<String, String>(topicName, key, value));
		try {
			System.out.println(value);
			Thread.sleep(500);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {

		BlackFridayProducer producer = new BlackFridayProducer(topicName);
		int lineCount = 0;
		FileInputStream fis;
		BufferedReader br = null;
		try {
			fis = new FileInputStream(fileName);
			br = new BufferedReader(new InputStreamReader(fis));
			String line = null;
			while ((line = br.readLine()) != null) {
				lineCount++;
				producer.sendMessage(lineCount + "", line);
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
}
