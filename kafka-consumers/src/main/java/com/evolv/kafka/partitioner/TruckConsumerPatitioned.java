package com.evolv.kafka.partitioner;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * 
 * @author chandra jagarlamudi
 *
 */
public class TruckConsumerPatitioned {

	public static void main(String[] args) {
		Properties consumerProperties = new Properties();
		consumerProperties.setProperty("bootstrap.servers", "localhost:9092");
		consumerProperties.setProperty("key.deserializer", IntegerDeserializer.class.getName());
		consumerProperties.setProperty("value.deserializer", StringDeserializer.class.getName());
		consumerProperties.setProperty("group.id", "truck-group");

		KafkaConsumer<Integer, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
		kafkaConsumer.subscribe(Collections.singletonList("truck-partition-topic"));
		
		try {
			while (true) {
				ConsumerRecords<Integer, String> truckCoordinates = kafkaConsumer.poll(Duration.ofSeconds(20));
				for (ConsumerRecord<Integer, String> coordinates : truckCoordinates) {
					System.out.println("Truck Id: " + coordinates.key());
					System.out.println("Truck Coordinates: " + coordinates.value());
					System.out.println("Partition: " + coordinates.partition());
				}
			}
		} finally {
			kafkaConsumer.close();
		}

	}

}