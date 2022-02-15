/**
 * 
 */
package com.evolv.kafka.partitioner;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author chandra jagarlamudi
 *
 */
public class TruckProducerPartioned {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Properties producerProperties = new Properties();
		producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
		producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerProperties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomTruckPartitioner.class.getName());

		KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(producerProperties);
		ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>("truck-partition-topic", 9, "37.2431 N, 115.793 E");

		try {
			// Asynchronous call, where Future is returned
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
		System.out.println("Success - Synchronous send message with custom partioner!!!");
	}

}
