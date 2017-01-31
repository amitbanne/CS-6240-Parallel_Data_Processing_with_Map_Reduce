package com.mapreduce.fine_lock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.mapreduce.model.Station;
import com.mapreduce.util.Util;

public class FineLockFib {
	// map of stationid and running TMAX count, record count
	public static Map<String, Station> stationTemperatureMap;
	// map of stationid and average TMAX
	public static Map<String, Double> stationAverageTMAX;
	// global lock object for threads to create new station record
	public static Integer lock = new Integer(1);
	
	// fine lock fibonacci starter method invoked by Loader class
	public static void fineLockFibStarter(List<String> unparsedData,int iterationCount){
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
		
		System.out.println("******* FINE LOCK FIBONACCI RESULTS *******");
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
		/*
		for(Entry<String,Double> e: stationAverageTMAX.entrySet()){
			System.out.println(e.getKey()+"->" +e.getValue());
		}*/
		
		
	}

	private static void parseTemperatureData(List<String> unparsedData) {
		// divide data between 4 threads
		int dataSize = unparsedData.size();
		List<String> t0Data = unparsedData.subList(0, dataSize / 4);
		List<String> t1Data = unparsedData.subList(dataSize / 4, dataSize / 2);
		List<String> t2Data = unparsedData.subList(dataSize / 2, (3 * dataSize / 4));
		List<String> t3Data = unparsedData.subList((3 * dataSize / 4), dataSize);
		
		// create 4 threads and pass the data share( Thread definition found in FineLock class)
		WorkerThreadFib t0 = new WorkerThreadFib(t0Data,"Thread-1");
		WorkerThreadFib t1 = new WorkerThreadFib(t1Data,"Thread-2");
		WorkerThreadFib t2 = new WorkerThreadFib(t2Data,"Thread-3");
		WorkerThreadFib t3 = new WorkerThreadFib(t3Data,"Thread-4");

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
class WorkerThreadFib extends Thread {
	// Thread's share of data to be processed
	private final List<String> unparsedTemparatureData;
	private String name;
	public WorkerThreadFib(List<String> unparsedTemparatureData, String name) {
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
			//FineLock.insertStationRecord(stationId, temperature);
			try{
				// if stationid exists, update the TMAX running count and record count
				// through a synchronized method of Station class, so that 
				// only that particular station object is locked for the thread 
				if (FineLockFib.stationTemperatureMap.containsKey(stationId)) {
					Station stationData = FineLockFib.stationTemperatureMap.get(stationId);
					stationData.addSynchrnonizedTemperature(temperature,true);
					FineLockFib.stationTemperatureMap.put(stationId, stationData);
				} else {
					// if stationid is not present, then thread should obtain
					// a station object so that no two threads can create a new object
					// for the station simultaneously, causing data loss
					Station stationData = new Station();
					synchronized (stationData) {
						// if two threads have entered the else part simultaneously, 
						// first thread created the new station object.
						// After second thread gets the execution chance, it should 
						// not create a new object again and overwrite first thread's data
						// Hence, do a exists check again to eliminate this situation
						if(FineLockFib.stationTemperatureMap.containsKey(stationId)){
							stationData = FineLockFib.stationTemperatureMap.get(stationId);
							stationData.addSynchrnonizedTemperature(temperature,true);
							FineLockFib.stationTemperatureMap.put(stationId, stationData);
						}else{
							stationData.addSynchrnonizedTemperature(temperature,false);
							FineLockFib.stationTemperatureMap.put(stationId, stationData);
						}
					}
				}
			}catch(Exception e){
				
			}
		}
	}

}
