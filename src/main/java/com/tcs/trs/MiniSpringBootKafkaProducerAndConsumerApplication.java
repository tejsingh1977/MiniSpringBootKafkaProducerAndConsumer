package com.tcs.trs;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.tcs.trs.producer.Consumer;

@SpringBootApplication
public class MiniSpringBootKafkaProducerAndConsumerApplication implements CommandLineRunner{

	public static void main(String[] args) {
		SpringApplication.run(MiniSpringBootKafkaProducerAndConsumerApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		//Producer.KafakaProducerMessage();
		  Consumer.runConsumer();
	}

}
