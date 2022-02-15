package com.evolv.kafka.avro;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

/**
 * 
 * @author chandra jagarlamudi
 *
 */
public class TruckConsumerAvroSerializer {

	public static void main(String[] args) {
		Properties consumerProperties = new Properties();
		consumerProperties.setProperty("bootstrap.servers", "localhost:9092");
		consumerProperties.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
		consumerProperties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
		consumerProperties.setProperty("group.id", "truck-avro-group");
		consumerProperties.setProperty("schema.registry.url", "http://localhost:8081");
		consumerProperties.setProperty("group.id", "truck-avro-group");
		consumerProperties.setProperty("specific.avro.reader", "true");

		KafkaConsumer<Integer, TruckCoordinates> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
		kafkaConsumer.subscribe(Collections.singletonList("truck-avro-topic"));

		ConsumerRecords<Integer, TruckCoordinates> truckCoordinates = kafkaConsumer.poll(Duration.ofSeconds(20));

		for (ConsumerRecord<Integer, TruckCoordinates> coordinates : truckCoordinates) {
			System.out.println("Truck# " + coordinates.key());
			System.out.println(coordinates.value().toString());
		}

		kafkaConsumer.close();

	}

}
