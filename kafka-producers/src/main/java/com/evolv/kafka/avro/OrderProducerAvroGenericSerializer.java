package com.evolv.kafka.avro;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * 
 * @author chandra jagarlamudi
 *
 */
public class OrderProducerAvroGenericSerializer {

	public static void main(String[] args) {
		
		Properties producerProperties = new Properties();
		producerProperties.setProperty("bootstrap.servers", "localhost:9092");
		producerProperties.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
		producerProperties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
		producerProperties.setProperty("schema.registry.url", "http://localhost:8081");
		
		KafkaProducer<String, GenericRecord> kafkaProducer = new KafkaProducer<>(producerProperties);
		
		Parser parser = new Schema.Parser();
		Schema schema = parser.parse("{\n"
				+ "\"namespace\": \"com.evolv.kafka.avro\",\n"
				+ "\"type\": \"record\",\n"
				+ "\"name\": \"Order\",\n"
				+ "\"fields\": [\n"
				+ "{\"name\": \"customerName\", \"type\": \"string\"},\n"
				+ "{\"name\": \"product\", \"type\": \"string\"},\n"
				+ "{\"name\": \"quantity\", \"type\": \"int\"}\n"
				+ "]\n"
				+ "}");
		
		Record order = new GenericData.Record(schema);
		order.put("customerName", "Chandra Jagarlamudi");
		order.put("product", "Mac Book Pro 16 inch");
		order.put("quantity", 100);
		
		ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>("order-avro-generic-topic", order.get("customerName").toString(), order);

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
		System.out.println("Success - Synchronous Avro generic send message!!!");
	}

}
