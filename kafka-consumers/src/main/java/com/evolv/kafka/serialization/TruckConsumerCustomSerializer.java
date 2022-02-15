package com.evolv.kafka.serialization;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import com.evolv.kafka.model.TruckCoordinates;

/**
 * 
 * @author chandra jagarlamudi
 *
 */
public class TruckConsumerCustomSerializer {

	public static void main(String[] args) {
		Properties consumerProperties = new Properties();
		consumerProperties.setProperty("bootstrap.servers", "localhost:9092");
		consumerProperties.setProperty("key.deserializer", IntegerDeserializer.class.getName());
		consumerProperties.setProperty("value.deserializer", TruckCoordinatesDeserializer.class.getName());
		consumerProperties.setProperty("group.id", "truckgroup");

		KafkaConsumer<Integer, TruckCoordinates> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
		kafkaConsumer.subscribe(Collections.singletonList("trucktopic"));

		try {
			while (true) {
				ConsumerRecords<Integer, TruckCoordinates> truckCoordinates = kafkaConsumer.poll(Duration.ofSeconds(20));

				for (ConsumerRecord<Integer, TruckCoordinates> coordinates : truckCoordinates) {
					System.out.println("Truck# " + coordinates.key());
					System.out.println(coordinates.value().toString());
				}
			}
		} finally {
			kafkaConsumer.close();
		}

	}

}
