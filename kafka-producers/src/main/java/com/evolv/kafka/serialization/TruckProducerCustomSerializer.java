/**
 * 
 */
package com.evolv.kafka.serialization;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;

import com.evolv.kafka.model.TruckCoordinates;

/**
 * @author chandra jagarlamudi
 *
 */
public class TruckProducerCustomSerializer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Properties producerProperties = new Properties();
		producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
		producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TruckCoordinatesSerializer.class.getName());
		
		TruckCoordinates truckCoordinates = new TruckCoordinates();
		truckCoordinates.setTruckId(9);
		truckCoordinates.setLatitude("22.5726 N");
		truckCoordinates.setLongitude("88.3639 E");

		KafkaProducer<Integer, TruckCoordinates> kafkaProducer = new KafkaProducer<>(producerProperties);
		ProducerRecord<Integer, TruckCoordinates> producerRecord = new ProducerRecord<>("trucktopic", 9, truckCoordinates);

		try {
			// synchronous call, where Future is returned
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
		System.out.println("Success - Synchronous, truck send message!!!");
	}

}

