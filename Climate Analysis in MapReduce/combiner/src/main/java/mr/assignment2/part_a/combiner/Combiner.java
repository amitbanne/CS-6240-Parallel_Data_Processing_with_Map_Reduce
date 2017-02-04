package mr.assignment2.part_a.combiner;

import java.io.IOException;

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

public class Combiner {

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
		job.setJarByClass(Combiner.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(TemperatureSumReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StationTemperatureData.class);
		job.setCombinerClass(TemperatureCombiner.class);
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
Mapper<Object, Text, Text, StationTemperatureData> {

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] records = value.toString().split("\n");
		// iterate through the records received in the map

		for (String record : records) {
			// if record contains neither TMAX nor TMIN data, skip processing
			if (!(record.contains("TMAX") || record.contains("TMIN")))
				continue;

			StationTemperatureData sData = null;
			// split the record on comma
			String[] tokens = record.split(",");
			Double temperature = Double.parseDouble(tokens[3]);
			// station Id
			String stationId = tokens[0];
			// extract the temperature and determine if it is TMAX or TMIN
			// perform in-mapper combining i.e. insert record data into a hash
			// map
			if (tokens[2].equals("TMAX")) {
				sData = new StationTemperatureData(0.0, temperature, 0, 1);
			} else {
				sData = new StationTemperatureData(temperature, 0.0, 1, 0);
			}
			// emit the record data(either TMAX or TMIN) for the station id
			context.write(new Text(stationId), sData);

		}

	}
}

class TemperatureSumReducer extends
Reducer<Text, StationTemperatureData, Text, NullWritable> {
	NullWritable nw = NullWritable.get();

	public void reduce(Text key, Iterable<StationTemperatureData> values,
			Context context) throws IOException, InterruptedException {
		// for a particular station, accumulate TMIN and TMAX
		Double tMin = 0.0, tMax = 0.0;
		int tMinCount = 0, tMaxCount = 0;

		// accumulate data by iterating on the input records
		for (StationTemperatureData sData : values) {
			tMin += sData.gettMin();
			tMinCount += sData.gettMinCount();

			tMax += sData.gettMax();
			tMaxCount += sData.gettMaxCount();
		}

		// compose the string output to be emitted by reducer
		StringBuilder output = new StringBuilder();
		output.append(key.toString());
		output.append(",");
		output.append((tMinCount == 0) ? "No TMIN Records" : (tMin / tMinCount));
		output.append(",");
		output.append((tMaxCount == 0) ? "No TMAX Records" : (tMax / tMaxCount));
		context.write(new Text(output.toString()), nw);
	}
}

class TemperatureCombiner extends
Reducer<Text, StationTemperatureData, Text, StationTemperatureData> {

	public void reduce(Text key, Iterable<StationTemperatureData> values,
			Context context) throws IOException, InterruptedException {
		// combine TMAX and TMIN for the particular station id
		Double tMin = 0.0, tMax = 0.0;
		Integer tMinCount = 0, tMaxCount = 0;
		// for all the incoming records, either the TMAX or TMIN count will be 1
		// all the time
		// as emitted in the same format by mapper
		for (StationTemperatureData sEntry : values) {
			if (sEntry.gettMaxCount() == 1) {
				tMaxCount += 1;
				tMax += sEntry.gettMax();
			} else {
				tMinCount += 1;
				tMin += sEntry.gettMin();
			}
		}
		// after accumulation of record values for station, emit the result
		context.write(new Text(key), new StationTemperatureData(tMin, tMax,
				tMinCount, tMaxCount));

	}
}