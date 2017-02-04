package mr.assignment2.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// This model is used for Secondary Sort, and represents the composite key

public class StationKey implements WritableComparable<StationKey>, Writable{
	
	private String stationId;
	private Integer year;
	
	public StationKey() {
		super();
	}

	public StationKey(String stationId, Integer year) {
		super();
		this.stationId = stationId;
		this.year = year;
	}

	public String getStationId() {
		return stationId;
	}

	public void setStationId(String stationId) {
		this.stationId = stationId;
	}

	public Integer getYear() {
		return year;
	}

	public void setYear(Integer year) {
		this.year = year;
	}

	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((stationId == null) ? 0 : stationId.hashCode());
		result = prime * result + ((year == null) ? 0 : year.hashCode());
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
		StationKey other = (StationKey) obj;
		if (stationId == null) {
			if (other.stationId != null)
				return false;
		} else if (!stationId.equals(other.stationId))
			return false;
		if (year == null) {
			if (other.year != null)
				return false;
		} else if (!year.equals(other.year))
			return false;
		return true;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.stationId = in.readUTF();
		this.year = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.stationId);
		out.writeInt(this.year);
	}

	
	@Override
	public int compareTo(StationKey sKey){
		return this.stationId.compareTo(sKey.stationId);
	}
	
}
