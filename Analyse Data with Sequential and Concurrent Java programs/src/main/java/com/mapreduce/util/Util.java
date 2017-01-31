package com.mapreduce.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.mapreduce.model.Station;

/**
 * This is a Util class that defines utility functions:
 * i) File parser method
 * ii) Method that computes Average TMAX for each station
 * iii) Fibonacci Util
 * */

public class Util {
	
	 // This method reads the input data file line by line(i.e. record by record),
	 // and, if the record contains TMAX data, only then it adds to the List of strings.
	 // Finally, this String list is returned after all data has been read 
	
	public static List<String> fileParser(String fileName) {
		BufferedReader reader = null;
		String line = "";
		// stores input file data, every record/line 
		// being a new element in the list 
		List<String> unparsedData = new ArrayList<>();
		try {
			// initialize reading the file
			reader = new BufferedReader(new FileReader(fileName));
			// read data line by line
			while ((line = reader.readLine()) != null) {
				unparsedData.add(line.trim());
			}
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return unparsedData;
	}
	
	  // This method iterates through input map, and computes average TMAX for it
	  // by calling Station class's computeAverageTemperature() method
	  // This data i.e. station id and its average TMAX value is inserted into the output map,
	  // and after completion of processing, the output map is returned 
	
	public static Map<String, Double> computeAverageTMAXForStation(Map<String, Station> stationTemperatureMap) {
		Map<String, Double> stationAverageTMAX = new HashMap<>();
		for (Entry<String, Station> sEntry : stationTemperatureMap.entrySet()) {
			Station sData = sEntry.getValue();
			Double averageTMAX = sData.computeAverageTemperature();
			stationAverageTMAX.put(sEntry.getKey(), averageTMAX);
		}
		return stationAverageTMAX;
	}

	// fibonacci calculator
	public static int fibonacciUtil(int n){

		if(n<=1)
			return n;
		else
			return (fibonacciUtil(n-1) + fibonacciUtil(n-2));
		
		/*int x = 0, y = 1, z = 1;
        for (int i = 0; i < n; i++) {
            x = y;
            y = z;
            z = x + y;
        }
        return x;*/
	}
}
