package com.tcs.trs.producer;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
	
public class Consumer {
	    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
	    private static KafkaConsumer<String, String> createConsumer()
	        {
	            final Properties consumerProps = new Properties();
	            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
	            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
	            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	            KafkaConsumer<String, String> ClickStreamConsumer = new KafkaConsumer<String, String>(consumerProps);  
	            ClickStreamConsumer.subscribe(Arrays.asList("mini-test-topic"));               
	            return ClickStreamConsumer;
	        }
	
	   public static void runConsumer() throws InterruptedException {
	        KafkaConsumer<String, String> streamConsumer = createConsumer();
	            final int NoOfTries = 5;   int RecordsCount = 0;
	            streamConsumer.poll(0);
	            streamConsumer.seekToBeginning(streamConsumer.assignment());
	            while (true) {
	                final ConsumerRecords<String, String> consumerRecords = streamConsumer.poll(30000);
	                if (consumerRecords.count()==0) {
	                    RecordsCount++;
	                            if (RecordsCount > NoOfTries)
	                                break;
	                            else
	                                continue;
	                }       
	                ConsumerRecord <String,String> CSRecord = null; 
	                for ( Iterator <ConsumerRecord<String,String>> record = consumerRecords.iterator(); record.hasNext();)
	                    {
	                    CSRecord = record.next();
	                         System.out.printf("Consumer Record: Key - " + CSRecord.key() + " Value - " + CSRecord.value()
	                         + " Partition - " + CSRecord.partition() + " Offset - " + CSRecord.offset() + "\n");
	                     }
	                CSRecord = null;
	                streamConsumer.commitAsync();
	            }
	            streamConsumer.close();
	            System.out.println("Consuming records is completed");
	        }
	
	}

