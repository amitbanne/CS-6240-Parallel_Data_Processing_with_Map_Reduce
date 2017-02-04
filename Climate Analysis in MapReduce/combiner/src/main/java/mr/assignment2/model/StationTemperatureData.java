package mr.assignment2.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

// This model is used for Combiner, In Mapper Combine, and Secondary SOrt

public class StationTemperatureData implements Writable {

	private Double tMin;
	private Double tMax;
	private Integer tMinCount;
	private Integer tMaxCount;

	public StationTemperatureData() {
	}

	public StationTemperatureData(Double tMin, Double tMax, Integer tMinCount,
			Integer tMaxCount) {
		super();
		this.tMin = tMin;
		this.tMax = tMax;
		this.tMinCount = tMinCount;
		this.tMaxCount = tMaxCount;
	}

	public Double gettMin() {
		return tMin;
	}

	public void settMin(Double tMin) {
		this.tMin = tMin;
	}

	public Double gettMax() {
		return tMax;
	}

	public void settMax(Double tMax) {
		this.tMax = tMax;
	}

	public Integer gettMinCount() {
		return tMinCount;
	}

	public void settMinCount(Integer tMinCount) {
		this.tMinCount = tMinCount;
	}

	public Integer gettMaxCount() {
		return tMaxCount;
	}

	public void settMaxCount(Integer tMaxCount) {
		this.tMaxCount = tMaxCount;
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		this.tMin = in.readDouble();
		this.tMax = in.readDouble();
		this.tMinCount = in.readInt();
		this.tMaxCount = in.readInt();

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(tMin);
		out.writeDouble(tMax);
		out.writeInt(tMinCount);
		out.writeInt(tMaxCount);
	}
	
	
	@Override
	public String toString() {
		return ", "+tMin + ", " + tMax;
	}
	
	
}
