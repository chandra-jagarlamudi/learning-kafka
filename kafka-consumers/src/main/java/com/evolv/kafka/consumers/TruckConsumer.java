package com.evolv.kafka.consumers;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * 
 * @author chandra jagarlamudi
 *
 */
public class TruckConsumer {

	public static void main(String[] args) {
		Properties consumerProperties = new Properties();
		consumerProperties.setProperty("bootstrap.servers", "localhost:9092");
		consumerProperties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
		consumerProperties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumerProperties.setProperty("group.id", "truck-group");

		KafkaConsumer<Integer, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
		kafkaConsumer.subscribe(Collections.singletonList("truck-topic"));
		
		try {
			while (true) {
				ConsumerRecords<Integer, String> orders = kafkaConsumer.poll(Duration.ofSeconds(20));

				for (ConsumerRecord<Integer, String> order : orders) {
					System.out.println("Truck# " + order.key());
					System.out.println("Location: " + order.value());
				}
			}
		} finally {
			kafkaConsumer.close();
		}
		
	}

}
