package com.mapreduce.prediction;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;


/** OrderingSamplingId performs ordering of records based on SAMPLING ID,
 *  in the order of appearance in UNLABLED DATA*/

public class OrderingSamplingId {

	public static void runOrder(String[] args) {

		Map<String, Integer> predictions = new HashMap<>();
		String unlabledPredFile = args[0];
		String unlabeledDataFile = args[1];
		
		try{
			
			BufferedReader br = new BufferedReader(new FileReader(unlabledPredFile));
			String line = "";
			while((line = br.readLine()) != null){
				String[] tokens = line.split("\t");
				predictions.put(tokens[0], Integer.parseInt(tokens[1]));
			}
			br.close();
			
			
			FileInputStream fin = new FileInputStream(unlabeledDataFile);
		    BufferedInputStream bis = new BufferedInputStream(fin);
		    CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(bis);
		    br = new BufferedReader(new InputStreamReader(input));

		    int first = 0;
			line = "";
			StringBuilder sb = new StringBuilder();
			sb.append("SAMPLING_EVENT_ID, SAW_AGELAIUS_PHOENICEUS");
			while((line = br.readLine()) != null){
				if(first == 0){
					first++;
					continue;
				}
				sb.append(System.lineSeparator());
				String samplingId = line.substring(0, line.indexOf(",")).trim();
				sb.append(samplingId);
				sb.append(",");
				sb.append(predictions.get(samplingId));
			}
			br.close();
			
			File file = new File("UNLABLED_PREDICTIONS.txt");

            // if file doesnt exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            // write in file
            bw.write(sb.toString());
            // close connection
            bw.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}

}
