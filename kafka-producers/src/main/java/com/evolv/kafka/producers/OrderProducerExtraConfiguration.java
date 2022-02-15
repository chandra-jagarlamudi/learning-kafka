package com.evolv.kafka.producers;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * 
 * @author chandra jagarlamudi
 *
 */
public class OrderProducerExtraConfiguration {

	public static void main(String[] args) {
		Properties producerProperties = new Properties();
		producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
		// How many brokers should receive the message before acknowledging values 0,1 or all
		producerProperties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
		// Buffer the messages before they are sent to the broker, it is in bytes default 256 MB
		producerProperties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "256");
		// Compression of messages, by default not compressed. Supported snappy | gzip | lz4
		producerProperties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
		//Retries
		producerProperties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
		//Retries backoff
		producerProperties.setProperty(ProducerConfig.RETRIES_CONFIG, "400");
		// Batch size default 16 KB
		producerProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "10243344432");
		// Linger
		producerProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "500");
		// Request timeout
		producerProperties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "200");
		// Idempotence
		producerProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		
		KafkaProducer<String, Integer> kafkaProducer = new KafkaProducer<>(producerProperties);
		ProducerRecord<String, Integer> producerRecord = new ProducerRecord<>("order-topic", "Mac Book Pro 16 inch", 9);

		try {
			// Asynchronous call, where Future is returned
			// asynchronousSend(kafkaProducer, producerRecord);

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
	
	@SuppressWarnings("unused")
	private static <K, V> void asynchronousSend(KafkaProducer<K, V> kafkaProducer,
			ProducerRecord<K, V> producerRecord) throws InterruptedException, ExecutionException {
		
		kafkaProducer.send(producerRecord, new OrderCallback());
		
	}

}
