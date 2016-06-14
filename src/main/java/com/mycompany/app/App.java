package com.mycompany.app;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;

public class App {

	public static void main(String[] args) {
		produce();
	}

	private static void produce() {
		System.out.println("Producer Test");

		// Kafka connection properties
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.0.150:9092");
		props.put("zk.connect", "localhost:2181");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		for (int i = 0; i < 10; i++) {
			System.out.println("Sending message: " + Integer.toString(i));
			producer.send(new ProducerRecord<String, String>("test_topic", Integer.toString(i),
					"Test string " + Integer.toString(i)));
			System.out.println("Sent message: " + Integer.toString(i));
		}
		producer.close();
		System.out.println("Producer closed");
	}

}