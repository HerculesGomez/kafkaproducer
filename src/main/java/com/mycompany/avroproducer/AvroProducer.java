package com.mycompany.avroproducer;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.Properties;
import java.util.Random;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AvroProducer {

	private static String[] usernames = {"herculesgomez", "billywizz", "charliechaplin", "bennybronco", "jimmyfish"};
	private static String[] maps = {"swamp", "cesspit", "bridge", "lake", "farm"};
	private static String[] gametypes = {"free for all", "team deathmatch", "capture the flag"};
	
	public static final String USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"headshotevent\","
            + "\"fields\":["
            + "  { \"name\":\"username\", \"type\":\"string\" },"
            + "  { \"name\":\"gametype\", \"type\":\"string\" },"
            + "  { \"name\":\"map\", \"type\":\"string\" },"
            + "  { \"name\":\"timestamp\", \"type\":\"long\" }"
            + "]}";
	
	public static void main(String[] args) {
		produce();
		consume();
		
	}

	private static void produce() {
		System.out.println("Starting Avro Producer");
		
		Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.150:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(USER_SCHEMA);
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);
        
        for (int i = 0; i < 10; i++) {
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

            ProducerRecord<String, byte[]> record = new ProducerRecord<>("headshotevent2", bytes);
            producer.send(record);

            try {
				Thread.sleep(250);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

        }
        producer.close();
        System.out.println("Closed Avro Producer");
		
	}

	private static void consume() {
		System.out.println("\n.......\n\nStarting Avro Consumer");
		
		SparkConf conf = new SparkConf()
                .setAppName("kafka-sandbox")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));

        Set<String> topics = Collections.singleton("headshotevent2");
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "192.168.0.150:9092");
        
        JavaPairInputDStream<String, byte[]> directKafkaStream = KafkaUtils.createDirectStream(ssc,
                String.class, byte[].class, StringDecoder.class, DefaultDecoder.class, kafkaParams, topics);

        directKafkaStream.foreachRDD(rdd -> {
            rdd.foreach(avroRecord -> {
                Schema.Parser parser = new Schema.Parser();
                Schema schema = parser.parse(USER_SCHEMA);
                Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
                GenericRecord record = recordInjection.invert(avroRecord._2).get();

                System.out.println("username= " + record.get("username")
                        + ", gametype= " + record.get("gametype")
                        + ", map= " + record.get("map")
                        + ", timestamp=" + record.get("timestamp"));
            });
        });

        ssc.start();
        ssc.awaitTermination();
        System.out.println("\n.......\nClosed Avro Consumer");
        
	}
}
