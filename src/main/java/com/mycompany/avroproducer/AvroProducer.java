package com.mycompany.avroproducer;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Properties;
import java.util.Random;
import java.util.Arrays;

public class AvroProducer {

	private static String[] usernames = { "herculesgomez", "billywizz", "charliechaplin", "bennybronco", "jimmyfish" };
	private static String[] maps = { "swamp", "cesspit", "bridge", "lake", "farm" };
	private static String[] gametypes = { "free for all", "team deathmatch", "capture the flag" };

	public static final String USER_SCHEMA = "{" + "\"type\":\"record\"," + "\"name\":\"headshotevent\","
			+ "\"fields\":[" + "{\"name\":\"username\",\"type\":\"string\"},"
			+ "{\"name\":\"gametype\",\"type\":\"string\"}," + "{\"name\":\"map\",\"type\":\"string\"},"
			+ "{\"name\":\"timestamp\",\"type\":\"long\"}" + "]}";

	private static Injection<GenericRecord, byte[]> recordInjection;

	static {
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(USER_SCHEMA);
		recordInjection = GenericAvroCodecs.toBinary(schema);
	}

	public static void main(String[] args) {
		produceAvroBytes();
		// produceAvroStrings();
		// consumeSpark();
		consumeAvroBytes();
	}

	private static void consumeAvroBytes() {
		System.out.println("\n\nStarting Avro Byte Consumer");

		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.0.150:9092");
		String consumeGroup = "cg1";
		props.put("group.id", consumeGroup);
		props.put("enable.auto.commit", "true");
		props.put("auto.offset.reset", "earliest");
		props.put("auto.commit.interval.ms", "100");
		props.put("heartbeat.interval.ms", "3000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

		KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(props);

		// Assign to specific topic and partition, subscribe could be used here
		// to subscribe to all topic.
		consumer.assign(Arrays.asList(new TopicPartition("headshotevent4", 0)));

		while (true) {

			ConsumerRecords<String, byte[]> records = consumer.poll(100);

			long lastOffset = 0;

			for (ConsumerRecord<String, byte[]> record : records) {
				GenericRecord convertedrecord = recordInjection.invert(record.value()).get();
				System.out.println(convertedrecord);
				lastOffset = record.offset();
			}

			System.out.println("lastOffset read: " + lastOffset);
			consumer.commitSync();

		}

	}

	private static void produceAvroBytes() {
		System.out.println("\n\nStarting Avro Producer");

		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.0.150:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(USER_SCHEMA);
		Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

		KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

		for (int i = 0; i < 1000; i++) {
			System.out.println("Sending record " + Integer.toString(i));
			Random random = new Random();
			java.util.Date today = new java.util.Date();
			java.sql.Timestamp ts1 = new java.sql.Timestamp(today.getTime());
			long tsTime1 = ts1.getTime();

			GenericData.Record avroRecord = new GenericData.Record(schema);
			avroRecord.put("username", usernames[random.nextInt(usernames.length)]);
			avroRecord.put("gametype", gametypes[random.nextInt(gametypes.length)]);
			avroRecord.put("map", maps[random.nextInt(maps.length)]);
			avroRecord.put("timestamp", tsTime1);

			byte[] bytes = recordInjection.apply(avroRecord);

			ProducerRecord<String, byte[]> record = new ProducerRecord<>("headshotevent4", bytes);
			producer.send(record);

		}
		producer.close();
		System.out.println("Closed Avro Producer");

	}

	private static void produceAvroStrings() {
		System.out.println("Starting Avro Producer");

		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.0.150:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(USER_SCHEMA);

		KafkaProducer<String, String> producer = new KafkaProducer<>(props);

		for (int i = 0; i < 1000; i++) {
			System.out.println("Sending record " + Integer.toString(i));
			Random random = new Random();
			java.util.Date today = new java.util.Date();
			java.sql.Timestamp ts1 = new java.sql.Timestamp(today.getTime());
			long tsTime1 = ts1.getTime();

			GenericData.Record avroRecord = new GenericData.Record(schema);
			avroRecord.put("username", usernames[random.nextInt(usernames.length)]);
			avroRecord.put("gametype", gametypes[random.nextInt(gametypes.length)]);
			avroRecord.put("map", maps[random.nextInt(maps.length)]);
			avroRecord.put("timestamp", tsTime1);

			ProducerRecord<String, String> record = new ProducerRecord<>("headshotevent2", avroRecord.toString());
			producer.send(record);

			// try {
			// Thread.sleep(250);
			// } catch (InterruptedException e) {
			// e.printStackTrace();
			// }

		}
		producer.close();
		System.out.println("Closed Avro Producer");

	}

	private static void consumeSpark() {
		// TODO
	}
}
