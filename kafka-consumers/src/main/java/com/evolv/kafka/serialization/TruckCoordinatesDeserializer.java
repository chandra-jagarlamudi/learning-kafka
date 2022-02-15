/**
 * 
 */
package com.evolv.kafka.serialization;

import java.io.IOException;

import org.apache.kafka.common.serialization.Deserializer;

import com.evolv.kafka.model.TruckCoordinates;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author chandra jagarlamudi
 *
 */
public class TruckCoordinatesDeserializer implements Deserializer<TruckCoordinates>{

	@Override
	public TruckCoordinates deserialize(String topic, byte[] data) {
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			return objectMapper.readValue(data, TruckCoordinates.class);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

}
