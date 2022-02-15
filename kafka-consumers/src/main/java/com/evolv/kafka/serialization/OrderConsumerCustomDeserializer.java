package com.evolv.kafka.serialization;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.evolv.kafka.model.Order;

/**
 * 
 * @author chandra jagarlamudi
 *
 */
public class OrderConsumerCustomDeserializer {

	public static void main(String[] args) {
		Properties consumerProperties = new Properties();
		consumerProperties.setProperty("bootstrap.servers", "localhost:9092");
		consumerProperties.setProperty("key.deserializer", StringDeserializer.class.getName());
		consumerProperties.setProperty("value.deserializer", OrderDeserializer.class.getName());
		consumerProperties.setProperty("group.id", "ordergroup");

		KafkaConsumer<String, Order> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
		kafkaConsumer.subscribe(Collections.singletonList("ordertopic"));

		try {
			while (true) {
				ConsumerRecords<String, Order> records = kafkaConsumer.poll(Duration.ofSeconds(20));

				for (ConsumerRecord<String, Order> record : records) {
					System.out.println("Customer name: " + record.key());
					System.out.println("Product name: " + record.value().getProduct());
					System.out.println("Quantity: " + record.value().getQuantity());
				}

			}
		} finally {
			kafkaConsumer.close();
		}

	}

}