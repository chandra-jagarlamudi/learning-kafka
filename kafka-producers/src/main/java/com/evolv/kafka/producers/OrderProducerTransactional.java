package com.evolv.kafka.producers;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * 
 * @author chandra jagarlamudi
 *
 */
public class OrderProducerTransactional {

	public static void main(String[] args) {
		Properties producerProperties = new Properties();
		producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
		producerProperties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "order-producer-1");
		
		KafkaProducer<String, Integer> kafkaProducer = new KafkaProducer<>(producerProperties);
		
		kafkaProducer.initTransactions();
		
		ProducerRecord<String, Integer> producerRecord = new ProducerRecord<>("order-topic", "Mac Book Pro 13 inch", 9);
		ProducerRecord<String, Integer> producerRecord2 = new ProducerRecord<>("order-topic", "Mac Book Air", 18);
		ProducerRecord<String, Integer> producerRecord3 = new ProducerRecord<>("order-topic", "Mac Book Pro 16 inch", 27);

		try {
			kafkaProducer.beginTransaction();

			kafkaProducer.send(producerRecord);
			kafkaProducer.send(producerRecord2);
			kafkaProducer.send(producerRecord3);

			kafkaProducer.commitTransaction();

		} catch (Exception e) {
			kafkaProducer.abortTransaction();
			e.printStackTrace();
		} finally {
			kafkaProducer.close();
		}

	}

}
