package mr.page_rank.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Record is a model used by reducer of JOB-3 to emit 
 * TOP K records in descending order of page rank value
 * */

public class Record implements Writable {
	private String node;
	private Double pageRank;
	
	public Record() {
		super();
	}

	public Record(String node, Double pageRank) {
		super();
		this.node = node;
		this.pageRank = pageRank;
	}

	public String getNode() {
		return node;
	}

	public void setNode(String node) {
		this.node = node;
	}

	public Double getPageRank() {
		return pageRank;
	}

	public void setPageRank(Double pageRank) {
		this.pageRank = pageRank;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.node);
		out.writeDouble(this.pageRank);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.node = in.readUTF();
		this.pageRank = in.readDouble();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((node == null) ? 0 : node.hashCode());
		result = prime * result
				+ ((pageRank == null) ? 0 : pageRank.hashCode());
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
		Record other = (Record) obj;
		if (node == null) {
			if (other.node != null)
				return false;
		} else if (!node.equals(other.node))
			return false;
		if (pageRank == null) {
			if (other.pageRank != null)
				return false;
		} else if (!pageRank.equals(other.pageRank))
			return false;
		return true;
	}

}
