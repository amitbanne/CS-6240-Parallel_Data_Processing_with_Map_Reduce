package com.mapreduce.model;

import com.mapreduce.util.Util;

/** Station Model
 *  records running TMAX count and record count  
 * */

public class Station {

	private Double cumulativeTemperature;
	private Integer readingCount;

	public Station() {
		super();
		cumulativeTemperature = 0.0;
		readingCount = 0;
	}

	public Double getCumulativeTemperature() {
		return cumulativeTemperature;
	}

	public void setCumulativeTemperature(Double cumulativeTemperature) {
		this.cumulativeTemperature = cumulativeTemperature;
	}

	public Integer getReadingCount() {
		return readingCount;
	}

	public void setReadingCount(Integer readingCount) {
		this.readingCount = readingCount;
	}

	public void addTemperature(Double temperature) {
		cumulativeTemperature += temperature;
		readingCount++;
	}

	public Double computeAverageTemperature() {
		return (cumulativeTemperature / readingCount);
	}
	
	// synchronized method for fine lock
	// lock is only on object and not on the entire data structure
	public synchronized void addSynchrnonizedTemperature(Double temperature, boolean fibonacci){
		if(fibonacci)
			Util.fibonacciUtil(17);
		
		cumulativeTemperature += temperature;
		readingCount++;
	}

	@Override
	public String toString() {
		return " [cumulativeTemperature=" + cumulativeTemperature + ", readingCount=" + readingCount + "]";
	}

	
	
}
