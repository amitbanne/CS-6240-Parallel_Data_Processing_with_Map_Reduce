package com.mapreduce.no_sharing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.mapreduce.model.Station;
import com.mapreduce.util.Util;

public class NoSharing {
	// map of stationid and running TMAX count, record count
	public static Map<String, Station> stationTemperatureMap;
	// map of stationid and average TMAX
	public static Map<String, Double> stationAverageTMAX;

	// no sharing starter method invoked by Loader class
	public static void noSharingStarter(List<String> unparsedData,
			int iterationCount) {
		List<Long> runningTimes = new ArrayList<Long>();
		// execute the process 10 times
		for (int i = 0; i < iterationCount; i++) {
			// initialize for each iteration
			stationTemperatureMap = new HashMap<String, Station>();
			long start = System.currentTimeMillis();
			initiateProcessing(unparsedData);
			long end = System.currentTimeMillis();
			runningTimes.add((end - start));
		}
		printData(runningTimes);
	}

	// compute average, min and max TMAX over 10 iterations and print
	private static void printData(List<Long> runningTimes) {
		long min = Collections.min(runningTimes);
		long max = Collections.max(runningTimes);

		long total = 0;
		for (long runTime : runningTimes)
			total += runTime;

		System.out.println("******* NO SHARING RESULTS *******");
		System.out.println("MIN RUNNING TIME(ms): " + min);
		System.out.println("MAX RUNNING TIME(ms): " + max);
		System.out.println("AVG RUNNING TIME(ms): "
				+ (total / runningTimes.size()));
		System.out.println("***********************************");
		System.out.println();
	}

	private static void initiateProcessing(List<String> unparsedData) {
		// parse and process data through threads
		parseTemperatureData(unparsedData);
		// compute average TMAX for each station
		stationAverageTMAX = Util
				.computeAverageTMAXForStation(stationTemperatureMap);
	}

	// merge processed data from threads into a common map
	// merging takes place one thread at a time
	public static void mergeRecordsForThread(
			Map<String, Station> temperatureData) {
		// for every stationid from thread's processed data,
		// check if stationid already exists in global map
		// if already present, update the existing value
		// else create a new record for station and insert data
		for (Entry<String, Station> stationEntry : temperatureData.entrySet()) {
			// if stationid already exists
			if (stationTemperatureMap.containsKey(stationEntry.getKey())) {
				// fetch existing TMAX and record count values
				Double existingTemperature = stationTemperatureMap.get(
						stationEntry.getKey()).getCumulativeTemperature();
				int existingRecordsCount = stationTemperatureMap.get(
						stationEntry.getKey()).getReadingCount();

				Double freshTemperature = stationEntry.getValue()
						.getCumulativeTemperature();
				int freshRecordsCount = stationEntry.getValue()
						.getReadingCount();
				// compute the updated TMAX and record count
				Double cumulativeTemperature = existingTemperature
						+ freshTemperature;
				int cumulativeRecordsCount = existingRecordsCount
						+ freshRecordsCount;

				Station station = stationTemperatureMap.get(stationEntry
						.getKey());
				station.setCumulativeTemperature(cumulativeTemperature);
				station.setReadingCount(cumulativeRecordsCount);
				stationTemperatureMap.put(stationEntry.getKey(), station);
			} else {
				// create a new station record, set TMAX and record count
				// and insert into map
				Station station = new Station();
				station.setCumulativeTemperature(stationEntry.getValue()
						.getCumulativeTemperature());
				station.setReadingCount(stationEntry.getValue()
						.getReadingCount());
				stationTemperatureMap.put(stationEntry.getKey(), station);
			}
		}
	}

	private static void parseTemperatureData(List<String> unparsedData) {
		// divide data between 4 threads
		int dataSize = unparsedData.size();
		List<String> t0Data = unparsedData.subList(0, dataSize / 4);
		List<String> t1Data = unparsedData.subList(dataSize / 4, dataSize / 2);
		List<String> t2Data = unparsedData.subList(dataSize / 2,
				(3 * dataSize / 4));
		List<String> t3Data = unparsedData
				.subList((3 * dataSize / 4), dataSize);

		// create 4 threads and pass the data share
		WorkerThread t0 = new WorkerThread(t0Data, "Thread-1");
		WorkerThread t1 = new WorkerThread(t1Data, "Thread-2");
		WorkerThread t2 = new WorkerThread(t2Data, "Thread-3");
		WorkerThread t3 = new WorkerThread(t3Data, "Thread-4");

		t0.start();
		t1.start();
		t2.start();
		t3.start();

		try {
			t0.join();
			t1.join();
			t2.join();
			t3.join();
			
			// merge data from all threads
			mergeRecordsForThread(t0.getStationTemperatureData());
			mergeRecordsForThread(t1.getStationTemperatureData());
			mergeRecordsForThread(t2.getStationTemperatureData());
			mergeRecordsForThread(t3.getStationTemperatureData());
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

class WorkerThread extends Thread {
	// Thread's share of data to be processed
	private final List<String> unparsedTemparatureData;
	// Thread's map of stationid vs running TMAX, record count data
	private Map<String, Station> stationTemperatureMap;
	private String name;

	public WorkerThread(List<String> unparsedTemparatureData, String name) {
		this.unparsedTemparatureData = unparsedTemparatureData;
		this.name = name;
		stationTemperatureMap = new HashMap<>();
	}
	
	public Map<String, Station> getStationTemperatureData(){
		return stationTemperatureMap;
	}

	public void run() {
		for (String data : unparsedTemparatureData) {
			if (!data.contains("TMAX"))
				continue;
			
			String[] tokens = data.split(",");
			String stationId = tokens[0];
			Double temperature = Double.parseDouble(tokens[3]);

			insertStationRecord(stationId, temperature);
		}
	}

	public void insertStationRecord(String stationId, Double temperature) {
		// if station already exists, update TMAX running sum and record count
		if (stationTemperatureMap.containsKey(stationId)) {
			Station stationData = stationTemperatureMap.get(stationId);
			stationData.addTemperature(temperature);
			stationTemperatureMap.put(stationId, stationData);
		}// else create a new station record in the map   
		else {
			Station stationData = new Station();
			stationData.addTemperature(temperature);
			stationTemperatureMap.put(stationId, stationData);
		}
	}
}
