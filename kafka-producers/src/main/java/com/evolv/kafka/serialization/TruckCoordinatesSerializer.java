/**
 * 
 */
package com.evolv.kafka.serialization;

import org.apache.kafka.common.serialization.Serializer;

import com.evolv.kafka.model.TruckCoordinates;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author chandra jagarlamudi
 *
 */
public class TruckCoordinatesSerializer implements Serializer<TruckCoordinates>{

	@Override
	public byte[] serialize(String topic, TruckCoordinates truckCoordinates) {
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			return objectMapper.writeValueAsString(truckCoordinates).getBytes();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			return null;
		}
	}

}
