package mr.assignment2.part_b.secondary_sort;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import mr.assignment2.comparator.GroupComparator;
import mr.assignment2.comparator.KeyComparator;
import mr.assignment2.model.StationKey;
import mr.assignment2.model.StationTemperatureData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SecondarySort {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err
					.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "mean temperature");
		job.setJarByClass(SecondarySort.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(TemperatureSumReducer.class);
		//Key Comparator configuration
		job.setSortComparatorClass(KeyComparator.class);
		// Group COmparator configuration
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setMapOutputKeyClass(StationKey.class);
		job.setMapOutputValueClass(StationTemperatureData.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

class TokenizerMapper extends
		Mapper<Object, Text, StationKey, StationTemperatureData> {

	// for in-mapper combining
	Map<StationKey, StationTemperatureData> stationTemperatureDataMap;
	
	// instantiate hashmap for in-mapper combining
	public void setup(Context context) {
		stationTemperatureDataMap = new HashMap<>();
	}

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		// iterate through the records received in the map
		String[] records = value.toString().split("\n");
		for (String record : records) {
			// if record contains neither TMAX nor TMIN data, skip processing
			if (!(record.contains("TMAX") || record.contains("TMIN")))
				continue;

			// split the record on comma
			String[] tokens = record.split(",");

			//station Id
			String stationId = tokens[0];
			// parse year data i.e. first 4 characters of second column
			Integer year = Integer.parseInt(tokens[1].trim().substring(0, 4));
			
			// composite key contains station id and year
			StationKey sKey = new StationKey(stationId, year);
			
			// extract the temperature and determine if it is TMAX or TMIN
			// perform in-mapper combining i.e. insert record data into a hash map
			Double temperature = Double.parseDouble(tokens[3]);
			if (tokens[2].equals("TMAX")) {
				updateTMAX(sKey, temperature);
			} else {
				updateTMIN(sKey, temperature);
			}
		}

	}
	// insert/update TMIN for a particular station and year
	private void updateTMIN(StationKey stationKey, Double temperature) {
		// if station id and year key doesnot exist, create a new record
		if(!stationTemperatureDataMap.containsKey(stationKey))
			stationTemperatureDataMap.put(stationKey, new StationTemperatureData(0.0,0.0,0,0));

		// if station id and year key exists, update the existing record
		StationTemperatureData sRecord = stationTemperatureDataMap.get(stationKey);
		sRecord.settMin(sRecord.gettMin()+temperature);
		sRecord.settMinCount(sRecord.gettMinCount()+1);
		stationTemperatureDataMap.put(stationKey, sRecord);
		
	}
	
	// insert/update TMAX for a particular station and year
	private void updateTMAX(StationKey stationKey, Double temperature) {
		// if station id and year key doesnot exist, create a new record
		if(!stationTemperatureDataMap.containsKey(stationKey))
			stationTemperatureDataMap.put(stationKey, new StationTemperatureData(0.0,0.0,0,0));
		
		// if station id and year key exists, update the existing record
		StationTemperatureData sRecord = stationTemperatureDataMap.get(stationKey);
		sRecord.settMax(sRecord.gettMax()+temperature);
		sRecord.settMaxCount(sRecord.gettMaxCount()+1);
		stationTemperatureDataMap.put(stationKey, sRecord);
	}
	
	// emit the accumulated data for station and year combination
	public void cleanup(Context context) throws IOException, InterruptedException { 
		
		for(Entry<StationKey, StationTemperatureData> sEntry : stationTemperatureDataMap.entrySet()){
			context.write(sEntry.getKey(), sEntry.getValue());
		}
	}
}

class TemperatureSumReducer extends
		Reducer<StationKey, StationTemperatureData, Text, NullWritable> {

	NullWritable nw = NullWritable.get();

	public void reduce(StationKey key, Iterable<StationTemperatureData> values,
			Context context) throws IOException, InterruptedException {

		// for a particular station, accumulate TMIN and TMAX per year
		Double tMin = 0.0, tMax = 0.0;
		int tMinCount = 0, tMaxCount = 0;
		
		// track year info between two records, so that when current year
		// is not the same as previous year, write previous year's temperature data
		// into a string, and start accumulation of temperature data for current year
		int currentYear = key.getYear(), prevYear = key.getYear();
		
		// compose station's temperature data output 
		StringBuilder output = new StringBuilder();
		output.append(key.getStationId());
		output.append(",[");
		String recordSeparator = "";
		for (StationTemperatureData sData : values) {
			prevYear = currentYear;
			currentYear = key.getYear();

			// if current record's year is not same as last record's
			// for the same station id, then write previous year data
			// into string
			if (prevYear != currentYear) {
				output.append(recordSeparator);
				recordSeparator = ",";
				output.append("(");
				output.append(prevYear);
				output.append(", ");
				output.append(((tMinCount == 0) ? "No TMIN Records"
						: (tMin / tMinCount)));
				output.append(", ");
				output.append(((tMaxCount == 0) ? "No TMAX Records"
						: (tMax / tMaxCount)));
				output.append(")");

				// reset counters for the current year
				tMax = 0.0;
				tMin = 0.0;
				tMaxCount = 0;
				tMinCount = 0;
			}
			// accumulate TMIN,TMAX and their counts
			tMax += sData.gettMax();
			tMaxCount += sData.gettMaxCount();
			tMin += sData.gettMin();
			tMinCount += sData.gettMinCount();
		}
		// for the last year's records , where previous year and current year will not change,
		// output the data to string
		if (prevYear == currentYear) {
			output.append(recordSeparator);
			recordSeparator = ",";
			output.append("(");
			output.append(prevYear);
			output.append(", ");
			output.append(((tMinCount == 0) ? "No TMIN Records"
					: (tMin / tMinCount)));
			output.append(", ");
			output.append(((tMaxCount == 0) ? "No TMAX Records"
					: (tMax / tMaxCount)));
			output.append(")");
		}
		output.append("]");
		
		// emit data for a particular station, that includes data over 10 years
		context.write(new Text(output.toString()), nw);
	}
}
