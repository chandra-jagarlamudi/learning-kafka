/**
 * 
 */
package com.evolv.kafka.producers;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author chandra jagarlamudi
 *
 */
public class OrderCallback implements Callback {

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		if(exception != null) {
			exception.printStackTrace();
		}
		
		System.out.println(metadata.partition());
		System.out.println(metadata.offset());
		System.out.println("Success - Asynchronous send message!!!");
	}

}
