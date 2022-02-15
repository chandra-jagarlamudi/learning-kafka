package com.evolv.kafka.consumers;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;

/**
 * 
 * @author chandra jagarlamudi
 *
 */
public class OrderConsumerExtraConfiguration {

	public static void main(String[] args) {
		Properties consumerProperties = new Properties();
		consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
		consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "order-group");
		// Minimum fetch size
		consumerProperties.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1024");
		// Fetch max wait 
		consumerProperties.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "200");
		//Heart beat interval
		consumerProperties.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
		//Session timeout 
		consumerProperties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "3000");
		//Max Partition Fetch bytes
		consumerProperties.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "1MB");
		//Auto offset reset (latest | earliest)
		consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		//Any unique value to log/track
		consumerProperties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "order-consumer");
		//Number of records to be returned when polled
		consumerProperties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
		//Partition assignment strategy (RangeAssignor | RoundRobin)
		consumerProperties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
		
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