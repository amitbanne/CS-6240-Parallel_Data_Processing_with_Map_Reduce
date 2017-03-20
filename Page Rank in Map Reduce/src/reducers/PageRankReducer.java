package mr.page_rank.reducers;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import mr.page_rank.counter.PageRankCounter;
import mr.page_rank.model.NodeData;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * PageRankReducer reads data from mapper and computes page rank scores,
 * from the input data passed through NodeData model for the node
 **/

public class PageRankReducer extends Reducer<Text, NodeData, Text, NullWritable> {
	
	private Set<String> nodeList;
	private Double danglingNodeScore;
	private final Double ALPHA = 0.15;
	private Long pageCount;
	private final Long DANGLING_NODE_SCORE_MULTIPLIER = 1000000000000l; // TO CONVERT previous iteration's dangling node scores back from long to double
	private NullWritable nw = NullWritable.get();
	
	public void setup(Context ctx){
		nodeList = new HashSet<>();
		pageCount = ctx.getConfiguration().getLong("PAGE_COUNT", -1);
		danglingNodeScore = Double.valueOf(ctx.getConfiguration().getLong("DANGLING_NODE_SCORE", -1));
		danglingNodeScore = danglingNodeScore/DANGLING_NODE_SCORE_MULTIPLIER;
	}
	
	public void reduce(Text key, Iterable<NodeData> vals, Context ctx) throws InterruptedException, IOException{
		String node = key.toString();
		String adjacencyList = "";
		Double prSummation = 0.0;
		
		// if record is of adjacency list, store the adjacency list for the node
		// if record has page rank value and outlink count of inlink of node, accumlate the values
		for(NodeData nData : vals){
			if(nData.isPageRankOrAdjacencyList()){
				if(nData.getOutLinkCount()!=0)
					prSummation+=(nData.getPageRankValue()/nData.getOutLinkCount());	
			}else{
				adjacencyList = nData.getAdjacencyList();
			}
		}
		// distribute the dangling node score of previous iteration amongst all the nodes in the current iteration
		Double danglingScoreDistribution = (danglingNodeScore/pageCount);
		
		// compute page rank score of the node for the current iteration
		Double pagerRankScore = (ALPHA/pageCount)+((1-ALPHA) * (danglingScoreDistribution + prSummation)); 
		
		// track total nodes in current iteration
		nodeList.add(node);
		
		// compose data in the required format and emit
		StringBuilder output = new StringBuilder();
		output.append(node);
		output.append("#");
		output.append(adjacencyList);
		output.append("#");
		output.append(pagerRankScore);
		
		ctx.write(new Text(output.toString()), nw);
		
	}
	
	public void cleanup(Context ctx){
		// set total nodes in the data set computed, in the counter to be used by next iteration
		ctx.getCounter(PageRankCounter.PAGE_COUNTER).setValue(nodeList.size());
	}
}
