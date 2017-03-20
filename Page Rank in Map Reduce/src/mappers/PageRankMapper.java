package mr.page_rank.mappers;

import java.io.IOException;

import mr.page_rank.counter.PageRankCounter;
import mr.page_rank.model.NodeData;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Object, Text, Text, NodeData> {
	
	private final Long DANGLING_NODE_SCORE_MULTIPLIER = 1000000000000l; // TO CONVERT dangling node scores from double to long and accumulate   
	private Integer currentIteration;
	private Long pageCount; // total page count from last iteration
	public void setup(Context ctx){
		// read fields that are set in configuration
		currentIteration = ctx.getConfiguration().getInt("ITERATION", -1);
		pageCount = (long) ctx.getConfiguration().getLong("PAGE_COUNT", -1);
	}
	
	public void map(Object _k, Text line, Context ctx) throws IOException, InterruptedException {
		if(line.toString().trim().isEmpty() || (!line.toString().contains("#")))
			return;
		
		// RECORD FORMAT: 
		// Z#A~B~C#PR_VALUE, where A,B,C are outlinks(adjacency list) of NODE Z
		
		// PARSE records according to the format, to process
		String[] tokens = line.toString().split("#");
		String node = tokens[0];
		Double pageRankValue = 0.0;
		
		// if this is first iteration, default page rank values will be 1/N
		// else read the existing page rank value from record and distribute amongst the outlinks of the node
		if(currentIteration == 0){
			pageRankValue = Double.valueOf(1.0/pageCount);	
		}else{
			pageRankValue = Double.parseDouble(tokens[2]);
		}
		
		// if current node is dangling node, accumulate its score in the global counter
		if(tokens[1].trim().length()==0){
			ctx.getCounter(PageRankCounter.DANGLING_NODE_SCORE).increment((long) (pageRankValue*DANGLING_NODE_SCORE_MULTIPLIER));
			ctx.write(new Text(node), new NodeData(false, tokens[1]));
		}// if current node is not a dangling node, distribute its page rank score amongst its outlinks
		else{
			String[] outlinks = tokens[1].split("~");
			for(String outlink : outlinks){
				ctx.write(new Text(outlink), new NodeData(true, pageRankValue, outlinks.length));
			}
			
			// emit node's adjacency list for reducer to output in the same format for mapper of next iteration to read, for consistency 
			ctx.write(new Text(node), new NodeData(false, tokens[1].trim()));
		}
		
	}
	

}
