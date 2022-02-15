/**
 * 
 */
package com.evolv.kafka.partitioner;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

/**
 * @author chandra jagarlamudi
 *
 */
public class CustomOrderPartitioner implements Partitioner {

	@Override
	public void configure(Map<String, ?> configs) {

	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
		if(key.toString().equals("Chandra Jagarlamudi")) {
			return 3;
		}
		return Math.abs((Utils.murmur2(keyBytes) % (partitions.size()-1)));
	}

	@Override
	public void close() {

	}

}
