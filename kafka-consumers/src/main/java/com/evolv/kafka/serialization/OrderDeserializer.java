/**
 * 
 */
package com.evolv.kafka.serialization;

import java.io.IOException;

import org.apache.kafka.common.serialization.Deserializer;

import com.evolv.kafka.model.Order;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author chandra jagarlamudi
 *
 */
public class OrderDeserializer implements Deserializer<Order>{

	@Override
	public Order deserialize(String topic, byte[] data) {
		Order order = null;
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			order = objectMapper.readValue(data, Order.class);
		} catch (IOException e) {
			e.printStackTrace();
		}
 		return order;
	}

}
