package com.evolv.kafka.avro;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;


/**
 * 
 * @author chandra jagarlamudi
 *
 */
public class OrderConsumerAvroGenericDeserializer {

	public static void main(String[] args) {
		Properties consumerProperties = new Properties();
		consumerProperties.setProperty("bootstrap.servers", "localhost:9092");
		consumerProperties.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
		consumerProperties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
		consumerProperties.setProperty("group.id", "order-avro-group");
		consumerProperties.setProperty("schema.registry.url", "http://localhost:8081");

		KafkaConsumer<String, GenericRecord> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
		kafkaConsumer.subscribe(Collections.singletonList("order-avro-generic-topic"));

		ConsumerRecords<String, GenericRecord> records = kafkaConsumer.poll(Duration.ofSeconds(20));

		for (ConsumerRecord<String, GenericRecord> record : records) {
			System.out.println("Customer name: " + record.key());
			System.out.println("Product name: " + record.value().get("product"));
			System.out.println("Quantity: " + record.value().get("quantity"));
		}

		kafkaConsumer.close();
	}

}