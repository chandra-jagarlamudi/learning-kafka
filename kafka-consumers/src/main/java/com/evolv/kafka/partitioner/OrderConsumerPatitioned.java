package com.evolv.kafka.partitioner;

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
public class OrderConsumerPatitioned {

	public static void main(String[] args) {
		Properties consumerProperties = new Properties();
		consumerProperties.setProperty("bootstrap.servers", "localhost:9092");
		consumerProperties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumerProperties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
		consumerProperties.setProperty("group.id", "order-group");

		KafkaConsumer<String, Integer> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
		kafkaConsumer.subscribe(Collections.singletonList("order-partition-topic"));
		
		try {
			while (true) {
				ConsumerRecords<String, Integer> orders = kafkaConsumer.poll(Duration.ofSeconds(20));
				for (ConsumerRecord<String, Integer> order : orders) {
					System.out.println("Product name: " + order.key());
					System.out.println("Quantity: " + order.value());
					System.out.println("Partition: " + order.partition());
				}
			}
		} finally {
			kafkaConsumer.close();
		}

	}

}