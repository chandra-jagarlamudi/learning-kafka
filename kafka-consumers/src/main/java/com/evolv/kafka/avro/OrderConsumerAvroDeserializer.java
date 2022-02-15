package com.evolv.kafka.avro;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;


/**
 * 
 * @author chandra jagarlamudi
 *
 */
public class OrderConsumerAvroDeserializer {

	public static void main(String[] args) {
		Properties consumerProperties = new Properties();
		consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
		consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
		consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "order-avro-group");
		consumerProperties.setProperty("schema.registry.url", "http://localhost:8081");
		consumerProperties.setProperty("specific.avro.reader", "true");

		KafkaConsumer<String, Order> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
		kafkaConsumer.subscribe(Collections.singletonList("order-avro-topic"));

		ConsumerRecords<String, Order> records = kafkaConsumer.poll(Duration.ofSeconds(20));

		for (ConsumerRecord<String, Order> record : records) {
			System.out.println("Headers: " + record.headers().toString());
			System.out.println("Customer name: " + record.key());
			System.out.println("Product name: " + record.value().getProduct());
			System.out.println("Quantity: " + record.value().getQuantity());
		}

		kafkaConsumer.close();
	}

}