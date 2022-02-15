/**
 * 
 */
package com.evolv.kafka.serialization;

import org.apache.kafka.common.serialization.Serializer;

import com.evolv.kafka.model.Order;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author chandrajagarlamudi
 *
 */
public class OrderSerializer implements Serializer<Order>{

	@Override
	public byte[] serialize(String topic, Order order) {
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			return objectMapper.writeValueAsString(order).getBytes();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			return null;
		}
	}

}
