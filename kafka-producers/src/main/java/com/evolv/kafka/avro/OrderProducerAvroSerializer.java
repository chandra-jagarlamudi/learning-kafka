package com.evolv.kafka.avro;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * 
 * @author chandra jagarlamudi
 *
 */
public class OrderProducerAvroSerializer {

	public static void main(String[] args) {
		Properties producerProperties = new Properties();
		producerProperties.setProperty("bootstrap.servers", "localhost:9092");
		producerProperties.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
		producerProperties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
		producerProperties.setProperty("schema.registry.url", "http://localhost:8081");

		Order order = new Order("Chandra Jagarlamudi", "Mac Book Pro 16 inch", 18);

		KafkaProducer<String, Order> kafkaProducer = new KafkaProducer<>(producerProperties);
		ProducerRecord<String, Order> producerRecord = new ProducerRecord<>("order-avro-topic", order.toString(), order);

		try {
			// Synchronous call, where RecordMetadata is returned
			synchronousSend(kafkaProducer, producerRecord);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			kafkaProducer.close();
		}

	}

	private static <K, V> void synchronousSend(KafkaProducer<K, V> kafkaProducer, ProducerRecord<K, V> producerRecord)
			throws InterruptedException, ExecutionException {
		RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
		System.out.println(recordMetadata.partition());
		System.out.println(recordMetadata.offset());
		System.out.println("Success - Synchronous Avro send message!!!");
	}

}
