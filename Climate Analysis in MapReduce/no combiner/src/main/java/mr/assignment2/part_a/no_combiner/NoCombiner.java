package mr.assignment2.part_a.no_combiner;

import java.io.IOException;

import mr.assignment2.model.StationEntry;

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

public class NoCombiner {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "mean temperature");
		job.setJarByClass(NoCombiner.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(TemperatureSumReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StationEntry.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

class TokenizerMapper extends Mapper<Object, Text, Text, StationEntry> {

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
			int tempType = 0;
			if(tokens[2].equals("TMAX"))
				tempType = 1;
			
			Double temperature = Double.parseDouble(tokens[3]);
			
			StationEntry sRecord = new StationEntry(tempType, temperature);
			// emit the record for the station 
			context.write(new Text(stationId), sRecord);
		}
	}
}

class TemperatureSumReducer extends Reducer<Text, StationEntry, Text, NullWritable> {
	NullWritable nw = NullWritable.get();
	
	public void reduce(Text key, Iterable<StationEntry> values, Context context)
			throws IOException, InterruptedException {
		// for a particular station, accumulate TMIN and TMAX
		int tMinCount = 0;
		double tMin = 0;
		
		int tMaxCount = 0;
		double tMax = 0;
		// accumulate data by iterating on the input records
		for (StationEntry sRecord : values) {
			if(sRecord.getTemperatureType() == 0){
				tMin+= sRecord.getTemperature();
				tMinCount++;
			}
			else{
				tMax+= sRecord.getTemperature();
				tMaxCount++;
			}
		}
		
		// compose the string output to be emitted by reducer
		StringBuilder output = new StringBuilder();
		output.append(key.toString());
		output.append(",");
		output.append(((tMinCount==0)? "No TMIN Records":(tMin/tMinCount)));
		output.append(",");
		output.append(((tMaxCount==0)? "No TMAX Records":(tMax/tMaxCount)));
		
		Text result = new Text();
		result.set(output.toString());
		
		context.write(result, nw);
	}
}