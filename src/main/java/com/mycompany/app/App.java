package com.mycompany.app;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class App {

	public static void main(String[] args) {

		System.out.println("Producer Test");

		// Kafka connection properties
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("zk.connect", "localhost:2181");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		for (int i = 0; i < 100; i++) {
			System.out.println("Sending message: " + Integer.toString(i));
			producer.send(
					new ProducerRecord<String, String>("headshotevent", Integer.toString(i), Integer.toString(i)));
			System.out.println("Sent message: " + Integer.toString(i));
		}
		producer.close();

	}
}