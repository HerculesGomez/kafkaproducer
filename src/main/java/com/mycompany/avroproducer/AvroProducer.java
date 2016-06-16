package com.mycompany.avroproducer;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Properties;
import java.util.Random;
import java.io.File;
import java.io.IOException;
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

	private static Schema.Parser parser = new Schema.Parser();
	private static Schema schema = parser.parse(USER_SCHEMA);

	static {
		recordInjection = GenericAvroCodecs.toBinary(schema);
	}

	public static void main(String[] args) throws IOException {
		produceAvroBytes(10);
		consumeAvroBytesToFile("output.avro");
		readAvroFile("output.avro");
		// produceAvroStrings();
		// consumeSpark();

	}

	private static void readAvroFile(String filename) throws IOException {
		System.out.println("\nReading records from avro file: " + filename);
		File file = new File(filename);
		DatumReader<GenericRecord> datumreader = new GenericDatumReader<GenericRecord>();
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumreader);
		GenericRecord record = null;

		while (dataFileReader.hasNext()) {
			record = dataFileReader.next(record);
			System.out.println(record);
		}
		dataFileReader.close();
		System.out.println("\nFinished reading records...");
	}

	private static void consumeAvroBytesToFile(String filename) throws IOException {
		System.out.println("\nStarting Avro Byte Consumer");

		Properties props = new Properties();
		//props.put("bootstrap.servers", "192.168.0.150:9092");
		props.put("bootstrap.servers", "192.168.93.130:9092");
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

		File file = new File(filename); // Create an output avro file to store
										// the avro records
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
		dataFileWriter.create(schema, file);

		int recordsprocessed = 0;
		long initialoffset = 0;

		while (true) {
			ConsumerRecords<String, byte[]> records = consumer.poll(100);

			long lastOffset = 0;

			for (ConsumerRecord<String, byte[]> record : records) {
				GenericRecord convertedrecord = recordInjection.invert(record.value()).get();
				dataFileWriter.append(convertedrecord); // Append the next

				lastOffset = record.offset();
				recordsprocessed++;

				// Set the intial offset
				if (initialoffset == 0) {
					initialoffset = lastOffset;
					System.out.println("Consuming messages beginning at offset " + String.valueOf(initialoffset));
				}

			}

			consumer.commitSync();

			// Break out of the loop if the offset is set to 0
			if (lastOffset == 0 && recordsprocessed > 0) {
				dataFileWriter.close(); // Close the avro file writer
				System.out.println("End of Kafka records beginning from offset " + String.valueOf(initialoffset)
						+ "\nNumber of comsumed messages: " + Integer.toString(recordsprocessed));
				System.out.println("Records written to " + filename);
				break;
			}

		}
	}

	private static void produceAvroBytes(int numRecords) {
		System.out.println("\nStarting Avro Producer");

		Properties props = new Properties();
		//props.put("bootstrap.servers", "192.168.0.150:9092");
		props.put("bootstrap.servers", "192.168.93.130:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

		KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

		for (int i = 0; i < numRecords; i++) {
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

	private static void produceAvroStrings(int numRecords) {
		System.out.println("\nStarting Avro Producer");

		Properties props = new Properties();
		// props.put("bootstrap.servers", "192.168.0.150:9092");
		props.put("bootstrap.servers", "192.168.93.130:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(USER_SCHEMA);

		KafkaProducer<String, String> producer = new KafkaProducer<>(props);

		for (int i = 0; i < numRecords; i++) {
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

			try {
				Thread.sleep(250);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
		producer.close();
		System.out.println("Closed Avro Producer");
	}
}
