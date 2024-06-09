package com.tcs.trs.producer;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Properties;
import java.util.Scanner;
import java.util.regex.Pattern;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {

	private final static String BOOTSTRAP_SERVERS = "localhost:9092";
	private static KafkaProducer<String, String> createProducer()
	{
		Properties propsClickStream = new Properties();
		propsClickStream.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		propsClickStream.put(ProducerConfig.CLIENT_ID_CONFIG, "ClickStreamProducer");
		propsClickStream.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());                
		propsClickStream.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		return new KafkaProducer<String, String>(propsClickStream);
	}
	
	
	public static void KafakaProducerMessage() throws InterruptedException, FileNotFoundException
	{
		final KafkaProducer<String, String> clickStreamProducer = createProducer();
		Scanner streamInput = new Scanner(new File("C:\\AWSWorkspace\\MiniSpringBootKafkaProducerAndConsumer\\AttendanceDataset1631604239079\\AttendanceDataset.csv"));
		while (streamInput.hasNextLine()) {
			String Record = streamInput.nextLine();
			String[] columns = Record.split(Pattern.quote(","));
			final ProducerRecord<String, String> record = new ProducerRecord<String, String>("mini-test-topic", columns[0], Record);
			clickStreamProducer.send(record);		
		}
		System.out.println("Completed producing records");
	}
	
	
}
