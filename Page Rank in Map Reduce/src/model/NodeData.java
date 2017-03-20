package mr.page_rank.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * NodeData is the model emitted my mapper of JOB-2
 * 
 * if pageRankOrAdjacencyList = true, record contains page rank value and outlinks count of inlink to current node
 * else if pageRankOrAdjacencyList = false, record contains adjacency list in string format of the current node
 **/
public class NodeData implements Writable {
	
	private Boolean pageRankOrAdjacencyList = new Boolean(false);
	private Double pageRankValue = new Double(0.0);
	private Integer outLinkCount = new Integer(0);
	private Text adjacencyList = new Text();
	
	public NodeData(){
	}
	
	public NodeData(Boolean pageRankOrAdjacencyList, String adjacencyList) {
		super();
		this.pageRankOrAdjacencyList = pageRankOrAdjacencyList;
		this.adjacencyList.set(adjacencyList);
		this.pageRankValue = 0.0;
		this.outLinkCount = 0;
	}
	
	public NodeData(Boolean pageRankOrAdjacencyList, Double pageRankValue,
			Integer outLinkCount) {
		super();
		this.pageRankOrAdjacencyList = pageRankOrAdjacencyList;
		this.pageRankValue = pageRankValue;
		this.outLinkCount = outLinkCount;
		this.adjacencyList = new Text();
	}


	public Boolean isPageRankOrAdjacencyList() {
		return pageRankOrAdjacencyList;
	}

	public void setPageRankOrAdjacencyList(Boolean pageRankOrAdjacencyList) {
		this.pageRankOrAdjacencyList = pageRankOrAdjacencyList;
	}

	public Double getPageRankValue() {
		return pageRankValue;
	}

	public void setPageRankValue(Double pageRankValue) {
		this.pageRankValue = pageRankValue;
	}

	public Integer getOutLinkCount() {
		return outLinkCount;
	}

	public void setOutLinkCount(Integer outLinkCount) {
		this.outLinkCount = outLinkCount;
	}

	public String getAdjacencyList() {
		return adjacencyList.toString();
	}

	public void setAdjacencyList(String adjacencyList) {
		this.adjacencyList.set(adjacencyList);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(this.pageRankOrAdjacencyList);
		out.writeDouble(this.pageRankValue);
		out.writeInt(this.outLinkCount);
		this.adjacencyList.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.pageRankOrAdjacencyList = in.readBoolean();
		this.pageRankValue = in.readDouble();
		this.outLinkCount = in.readInt();
		this.adjacencyList.readFields(in);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((adjacencyList == null) ? 0 : adjacencyList.hashCode());
		result = prime * result
				+ ((outLinkCount == null) ? 0 : outLinkCount.hashCode());
		result = prime
				* result
				+ ((pageRankOrAdjacencyList == null) ? 0
						: pageRankOrAdjacencyList.hashCode());
		result = prime * result
				+ ((pageRankValue == null) ? 0 : pageRankValue.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		NodeData other = (NodeData) obj;
		if (adjacencyList == null) {
			if (other.adjacencyList != null)
				return false;
		} else if (!adjacencyList.toString().equals(other.adjacencyList.toString()))
			return false;
		if (outLinkCount == null) {
			if (other.outLinkCount != null)
				return false;
		} else if (!outLinkCount.equals(other.outLinkCount))
			return false;
		if (pageRankOrAdjacencyList == null) {
			if (other.pageRankOrAdjacencyList != null)
				return false;
		} else if (!pageRankOrAdjacencyList
				.equals(other.pageRankOrAdjacencyList))
			return false;
		if (pageRankValue == null) {
			if (other.pageRankValue != null)
				return false;
		} else if (!pageRankValue.equals(other.pageRankValue))
			return false;
		return true;
	}

	
}
