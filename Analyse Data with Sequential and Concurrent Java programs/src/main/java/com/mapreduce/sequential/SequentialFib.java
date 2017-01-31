package com.mapreduce.sequential;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mapreduce.model.Station;
import com.mapreduce.util.Util;

public class SequentialFib {
	
	// map of stationid and running TMAX count, record count
	public static Map<String, Station> stationTemperatureMap;
	// map of stationid and average TMAX
	public static Map<String, Double> stationAverageTMAX;
	
	// sequential fibonacci starter method invoked by Loader class
	public static void sequentialFibStarter(List<String> unparsedData,int iterationCount){
		List<Long> runningTimes = new ArrayList<Long>();
		// execute the process 10 times
		for(int i=0;i<iterationCount;i++){
			// initialize for each iteration
			stationTemperatureMap = new HashMap<String, Station>();
			long start = System.currentTimeMillis();
			initiateProcessing(unparsedData);
			long end = System.currentTimeMillis();
			runningTimes.add((end-start));
		}
		printData(runningTimes);
	}
	
	// compute average, min and max TMAX over 10 iterations and print
	private static void printData(List<Long> runningTimes) {
		long min = Collections.min(runningTimes);
		long max = Collections.max(runningTimes);
		
		long total = 0;
		for(long runTime : runningTimes)
			total+=runTime;
		
		System.out.println("******* SEQUENTIAL FIBONACCI RESULTS *******");
		System.out.println("MIN RUNNING TIME(ms): "+min);
		System.out.println("MAX RUNNING TIME(ms): "+max);
		System.out.println("AVG RUNNING TIME(ms): "+(total/runningTimes.size()));
		System.out.println("***********************************");
		System.out.println();
	}

	private static void initiateProcessing(List<String> unparsedData) {
		// parse and process data through threads
		parseTemperatureData(unparsedData);
		// compute average TMAX for each station
		stationAverageTMAX = Util.computeAverageTMAXForStation(stationTemperatureMap);
	}
	// parse station data, line by line
	public static void parseTemperatureData(List<String> unparsedData) {
		for (String data : unparsedData) {
			if (!data.contains("TMAX"))
				continue;
			
			String[] tokens = data.split(",");
			String stationID = tokens[0];
			Double temperature = Double.parseDouble(tokens[3]);
			
			//insert data into global map
			processTemperatureRecord(stationID, temperature);
		}

	}
	// insert/update (running) TMAX and record count
	public static void processTemperatureRecord(String stationID, Double temperature) {
		
		// if station already exists, update TMAX running sum and record count
		if (stationTemperatureMap.containsKey(stationID)) {
			//delay
			Util.fibonacciUtil(17);
			
			Station stationData = stationTemperatureMap.get(stationID);
			stationData.addTemperature(temperature);
			stationTemperatureMap.put(stationID, stationData);
		}// else create a new station record in the map 
		else {
			Station stationData = new Station();
			stationData.addTemperature(temperature);
			stationTemperatureMap.put(stationID, stationData);
		}
	}
}