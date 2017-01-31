package com.mapreduce.no_lock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mapreduce.model.Station;
import com.mapreduce.util.Util;

public class NoLock {
	// map of stationid and running TMAX count, record count
	public static Map<String, Station> stationTemperatureMap;
	// map of stationid and average TMAX
	public static Map<String, Double> stationAverageTMAX;
	
	// no lock starter method invoked by Loader class
	public static void noLockStarter(List<String> unparsedData, int iterationCount){
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
		
		System.out.println("******* NO LOCK RESULTS *******");
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

	private static void parseTemperatureData(List<String> unparsedData) {
		// divide data between 4 threads
		int dataSize = unparsedData.size();
		List<String> t0Data = unparsedData.subList(0, dataSize / 4);
		List<String> t1Data = unparsedData.subList(dataSize / 4, dataSize / 2);
		List<String> t2Data = unparsedData.subList(dataSize / 2, (3 * dataSize / 4));
		List<String> t3Data = unparsedData.subList((3 * dataSize / 4), dataSize);

		// create 4 threads and pass the data share
		WorkerThread t0 = new WorkerThread(t0Data,"Thread-1");
		WorkerThread t1 = new WorkerThread(t1Data,"Thread-2");
		WorkerThread t2 = new WorkerThread(t2Data,"Thread-3");
		WorkerThread t3 = new WorkerThread(t3Data,"Thread-4");

		t0.start();
		t1.start();
		t2.start();
		t3.start();

		try {
			t0.join();
			t1.join();
			t2.join();
			t3.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}
}

class WorkerThread extends Thread {
	// Thread's share of data to be processed
	private final List<String> unparsedTemparatureData;
	private String name;
	public WorkerThread(List<String> unparsedTemparatureData,String name) {
		this.unparsedTemparatureData = unparsedTemparatureData;
		this.name = name;
	}

	public void run() {
		// for each line of data, parse and extract stationid and TMAX
		for (String data : unparsedTemparatureData) {
			if (!data.contains("TMAX"))
				continue;
			
			String[] tokens = data.split(",");
			String stationId = tokens[0];
			Double temperature = Double.parseDouble(tokens[3]);

			// if station already exists, update TMAX running sum and record count
			// no synchronization present
			if (NoLock.stationTemperatureMap.containsKey(stationId)) {
				Station stationData = NoLock.stationTemperatureMap.get(stationId);
				stationData.addTemperature(temperature);
				NoLock.stationTemperatureMap.put(stationId, stationData);
			}// else create a new station record in the map  
			else {
				Station stationData = new Station();
				stationData.addTemperature(temperature);
				NoLock.stationTemperatureMap.put(stationId, stationData);
			}
		}
	}
}