package mr.page_rank.starter;


import mr.page_rank.comparator.KeyComparator;
import mr.page_rank.counter.PageRankCounter;
import mr.page_rank.mappers.PageRankMapper;
import mr.page_rank.mappers.ParserMapper;
import mr.page_rank.mappers.TopKMapper;
import mr.page_rank.model.NodeData;
import mr.page_rank.partitioner.ResultPartitioner;
import mr.page_rank.reducers.PageRankReducer;
import mr.page_rank.reducers.TopKReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank {

	private static Path input;
	private static Long pageCounter = 0L;
	private static Long danglingNodeScore = 0L;
	private static Configuration conf;
	private static final int MAX_ITERATIONS = 10;
	private static final int TOP_K = 100;
	public static void main(String[] args) {

		conf = new Configuration();
		String[] otherArgs = null;
		try {
			otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

			if (otherArgs.length < 2) {
				System.out.println("Please specify input path and output path");
				System.exit(0);
			}

			// pre-processing
			preProcess(conf, otherArgs[0]);
			System.out.println("PRE PROCESSING COMPLETE");
			
			// iterate
			for(int iteration =0; iteration < MAX_ITERATIONS ;iteration++){
				System.out.println("ITERATION: "+iteration);
				computePageRank(conf, iteration, otherArgs[0]);
			}
			
			// TOP 100
			topKResults(conf, otherArgs[0], otherArgs[1]);
			
			System.out.println("ALL DONE");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	// This job outputs the top 100 results in descending order
	private static void topKResults(Configuration conf, String input, String output) throws Exception {
		
		// set inputs to be used in mappers and reducers
		conf.set("TOP_K", TOP_K+"");
		
		Job job = Job.getInstance(conf, "top k results");
		job.setJarByClass(PageRank.class);
		
		job.setMapperClass(TopKMapper.class);
		job.setReducerClass(TopKReducer.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setPartitionerClass(ResultPartitioner.class);
		
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		
		StringBuilder ip = new StringBuilder();
		ip.append(input);
		ip.append("/page_rank_output");
		ip.append("/PR-"+(MAX_ITERATIONS-1));
		//ip.append("/part-r-00000");
		
		FileInputFormat.addInputPath(job, new Path(ip.toString()));
		FileOutputFormat.setOutputPath(job, new Path(output));

		boolean ok = job.waitForCompletion(true);
		if (!ok) {
			throw new Exception("Job failed");
		}
		
	}

	// This is a map only job to parse BZ2 input file
	public static void preProcess(Configuration conf, String input)
			throws Exception {

		Job job = Job.getInstance(conf, "parse input");
		job.setJarByClass(PageRank.class);
		
		job.setMapperClass(ParserMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(1);
		
		// configure output directory for the job
		StringBuilder op = new StringBuilder();
		op.append(input);
		op.append("/parser_output");
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(op.toString()));

		boolean ok = job.waitForCompletion(true);
		if (!ok) {
			throw new Exception("Job failed");
		}
		
		// read the values from counter, to be used as inputs to JOB-2
		
		Counters counters = job.getCounters();
		pageCounter = counters.findCounter(PageRankCounter.PAGE_COUNTER).getValue();
		
	}

	// This job computes page rank values, and is executed 10 times
	public static void computePageRank(Configuration conf, int iteration, String input) throws Exception{
		
		// set inputs to be used in mappers and reducers
		conf.setInt("ITERATION",iteration);
		conf.setLong("PAGE_COUNT", pageCounter);
		conf.setLong("DANGLING_NODE_SCORE", (iteration==0?0:danglingNodeScore));
		
		Job job = Job.getInstance(conf, "compute page rank");
		job.setJarByClass(PageRank.class);
		
		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NodeData.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		StringBuilder ip = new StringBuilder();
		StringBuilder output = new StringBuilder();
		
		// if first iteration, output of parser job is the input
		if(iteration==0){
			ip.append(input);
			ip.append("/parser_output");
			//ip.append("/part-r-00000");
		}// if not first iteration, output of previous iteration is the input
		else{
			ip.append(input);
			ip.append("/page_rank_output");
			ip.append("/PR-"+(iteration-1));
			//ip.append("/part-r-00000");
		}
		
		// configure output directory for the current iteration
		output.append(input);
		output.append("/page_rank_output");
		output.append("/PR-"+iteration);
		
		FileInputFormat.addInputPath(job, new Path(ip.toString()));
		FileOutputFormat.setOutputPath(job, new Path(output.toString()));

		boolean ok = job.waitForCompletion(true);
		if (!ok) {
			throw new Exception("Job failed");
		}
		
		// read the values from counter, to be used as inputs to next iteration
		
		Counters counters = job.getCounters();
		pageCounter = counters.findCounter(PageRankCounter.PAGE_COUNTER).getValue();
		danglingNodeScore = counters.findCounter(PageRankCounter.DANGLING_NODE_SCORE).getValue();

	}

}
