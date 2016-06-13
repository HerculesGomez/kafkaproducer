package com.mycompany.app;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;

public class App {

	public static void main(String[] args) {

		produce();
		// consume();

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

		for (int i = 0; i < 100; i++) {
			System.out.println("Sending message: " + Integer.toString(i));
			producer.send(new ProducerRecord<String, String>("test_topic", Integer.toString(i),
					"Test string " + Integer.toString(i)));
			System.out.println("Sent message: " + Integer.toString(i));
		}
		producer.close();
		System.out.println("Producer closed");
	}

	private static void consume() {
		///// Consumer currently not working
		System.out.println("\n--------------------------------------");
		System.out.println("Starting Consumer");

		Properties props2 = new Properties();
		props2.put("bootstrap.servers", "192.168.0.150:9092");
		props2.put("group.id", "test");
		props2.put("enable.auto.commit", "true");
		props2.put("auto.commit.interval.ms", "1000");
		props2.put("session.timeout.ms", "30000");
		props2.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props2.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props2);
		consumer.subscribe(Arrays.asList("headshotevent"));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records)
				System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
		}
	}
}
