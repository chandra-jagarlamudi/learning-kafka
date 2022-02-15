/**
 * 
 */
package com.evolv.kafka.model;

/**
 * @author chandra jagarlamudi
 *
 */
public class TruckCoordinates {

	private Long truckId;
	private String latitude;
	private String longitude;

	public Long getTruckId() {
		return truckId;
	}

	public void setTruckId(Long truckId) {
		this.truckId = truckId;
	}

	public String getLatitude() {
		return latitude;
	}

	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}

	public String getLongitude() {
		return longitude;
	}

	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}

	@Override
	public String toString() {
		return "TruckCoordinates [truckId=" + truckId + ", latitude=" + latitude + ", longitude=" + longitude + "]";
	}

}
