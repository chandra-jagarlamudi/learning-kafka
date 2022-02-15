package com.evolv.kafka.serialization;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import com.evolv.kafka.model.Order;

/**
 * 
 * @author chandra jagarlamudi
 *
 */
public class OrderProducerCustomSerializer {

	public static void main(String[] args) {
		Properties producerProperties = new Properties();
		producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrderSerializer.class.getName());
		
		Order order = new Order();
		order.setCustomerName("Chandra Jagarlamudi");
		order.setProduct("Mac Book Pro 16 inch");
		order.setQuantity(9);

		KafkaProducer<String, Order> kafkaProducer = new KafkaProducer<>(producerProperties);
		ProducerRecord<String, Order> producerRecord = new ProducerRecord<>("ordertopic", order.getCustomerName(), order);

		try {
			// Synchronous call, where RecordMetadata is returned
			synchronousSend(kafkaProducer, producerRecord);
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			kafkaProducer.close();
		}

	}

	private static <K, V> void synchronousSend(KafkaProducer<K, V> kafkaProducer,
			ProducerRecord<K, V> producerRecord) throws InterruptedException, ExecutionException {
		RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
		System.out.println(recordMetadata.partition());
		System.out.println(recordMetadata.offset());
		System.out.println("Success - Synchronous send message!!!");
	}

}
