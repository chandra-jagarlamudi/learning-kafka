/**
 * 
 */
package com.evolv.kafka.producers;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author chandra jagarlamudi
 *
 */
public class TruckProducer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Properties producerProperties = new Properties();
		producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
		producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

		KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(producerProperties);
		ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>("truck-topic", 9, "22.5726 N, 88.3639 E");

		try {
			// Asynchronous call, where Future is returned
			asynchronousSend(kafkaProducer, producerRecord);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			kafkaProducer.close();
		}

	}

	private static <K, V> void asynchronousSend(KafkaProducer<K, V> kafkaProducer, ProducerRecord<K, V> producerRecord)
			throws InterruptedException, ExecutionException {

		kafkaProducer.send(producerRecord, new TruckCallback());

	}

}
