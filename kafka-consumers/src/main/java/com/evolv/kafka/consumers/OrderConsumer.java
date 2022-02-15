package com.evolv.kafka.consumers;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
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
public class OrderConsumer {

	public static void main(String[] args) {
		Properties consumerProperties = new Properties();
		consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
		consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "order-group");

		KafkaConsumer<String, Integer> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
		kafkaConsumer.subscribe(Collections.singletonList("order-topic"));
		
		try {
			while (true) {
				ConsumerRecords<String, Integer> orders = kafkaConsumer.poll(Duration.ofSeconds(20));
				for (ConsumerRecord<String, Integer> order : orders) {
					System.out.println("Product name: " + order.key());
					System.out.println("Quantity: " + order.value());
				}
			}
		} finally {
			kafkaConsumer.close();
		}

	}

}