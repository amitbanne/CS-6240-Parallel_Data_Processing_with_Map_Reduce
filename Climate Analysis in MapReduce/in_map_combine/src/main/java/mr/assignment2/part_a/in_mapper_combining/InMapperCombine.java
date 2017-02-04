package mr.assignment2.part_a.in_mapper_combining;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import mr.assignment2.model.StationTemperatureData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InMapperCombine {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "mean temperature");
		job.setJarByClass(InMapperCombine.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(TemperatureSumReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StationTemperatureData.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

class TokenizerMapper extends Mapper<Object, Text, Text, StationTemperatureData> {

	// for in-mapper combining
	Map<String, StationTemperatureData> stationTemperatureDataMap;

	// instantiate hashmap for in-mapper combining
	public void setup(Context context) {
		stationTemperatureDataMap = new HashMap<>();
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// iterate through the records received in the map
		String[] records = value.toString().split("\n");
		for(String record : records){
			// if record contains neither TMAX nor TMIN data, skip processing
			if(!(record.contains("TMAX") || record.contains("TMIN")))
				continue;
			
			// split the record on comma
			String[] tokens = record.split(",");
			String stationId = tokens[0];
			// extract the temperature and determine if it is TMAX or TMIN
			// perform in-mapper combining i.e. insert record data into a hash map
			Double temperature = Double.parseDouble(tokens[3]);
			
			if(tokens[2].equals("TMAX")){
				updateTMAX(stationId, temperature);
			}else{
				updateTMIN(stationId, temperature);
			}
		}
	}
	
	// insert/update TMIN for a particular station and year
	private void updateTMIN(String stationId, Double temperature) {
		// if station id and year key doesnot exist, create a new record
		if(!stationTemperatureDataMap.containsKey(stationId))
			stationTemperatureDataMap.put(stationId, new StationTemperatureData(0.0,0.0,0,0));
		
		// if station id and year key exists, update the existing record
		StationTemperatureData sRecord = stationTemperatureDataMap.get(stationId);
		sRecord.settMin(sRecord.gettMin()+temperature);
		sRecord.settMinCount(sRecord.gettMinCount()+1);
		stationTemperatureDataMap.put(stationId, sRecord);
		
	}
	// insert/update TMAX for a particular station and year
	private void updateTMAX(String stationId, Double temperature) {
		// if station id and year key doesnot exist, create a new record
		if(!stationTemperatureDataMap.containsKey(stationId))
			stationTemperatureDataMap.put(stationId, new StationTemperatureData(0.0,0.0,0,0));
		
		// if station id and year key exists, update the existing record
		StationTemperatureData sRecord = stationTemperatureDataMap.get(stationId);
		sRecord.settMax(sRecord.gettMax()+temperature);
		sRecord.settMaxCount(sRecord.gettMaxCount()+1);
		stationTemperatureDataMap.put(stationId, sRecord);
	}
	
	// emit the accumulated data for station and year combination
	public void cleanup(Context context) throws IOException, InterruptedException { 
		
		for(Entry<String, StationTemperatureData> sEntry : stationTemperatureDataMap.entrySet()){
			context.write(new Text(sEntry.getKey()), sEntry.getValue());
		}
	}
}


class TemperatureSumReducer extends Reducer<Text, StationTemperatureData, Text,NullWritable> {
	NullWritable nw = NullWritable.get();
	public void reduce(Text key, Iterable<StationTemperatureData> values, Context context)
			throws IOException, InterruptedException {
		
		// for a particular station, accumulate TMIN and TMAX
		Double tMin = 0.0, tMax = 0.0;
		int tMinCount = 0, tMaxCount = 0;
		// accumulate data by iterating on the input records		
		for(StationTemperatureData sData : values){
			tMin+=sData.gettMin();
			tMinCount+=sData.gettMinCount();
			
			tMax+=sData.gettMax();
			tMaxCount+=sData.gettMaxCount();
		}
		
		// compose the string output to be emitted by reducer
		StringBuilder output = new StringBuilder();
		output.append(key.toString());
		output.append(",");
		output.append((tMinCount==0)?"No TMIN Records":(tMin/tMinCount));
		output.append(",");
		output.append((tMaxCount==0)?"No TMAX Records":(tMax/tMaxCount));
		
		context.write(new Text(output.toString()),nw);
	}
}
