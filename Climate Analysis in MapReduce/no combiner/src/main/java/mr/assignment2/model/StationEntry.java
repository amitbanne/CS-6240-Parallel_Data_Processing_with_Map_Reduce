package mr.assignment2.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

// This model is used only for NoCombiner

public class StationEntry implements Writable {

	private int temperatureType; // 0 - TMIN and 1 - TMAX
	private double temperature;

	public StationEntry(){
		
	}
	
	public StationEntry(int temperatureType, double temperature) {
		super();
		this.temperatureType = temperatureType;
		this.temperature = temperature;
	}

	public int getTemperatureType() {
		return temperatureType;
	}

	public void setTemperatureType(int temperatureType) {
		this.temperatureType = temperatureType;
	}

	public double getTemperature() {
		return temperature;
	}

	public void setTemperature(double temperature) {
		this.temperature = temperature;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.temperatureType = in.readInt();
		this.temperature = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(temperatureType);
		out.writeDouble(temperature);

	}

}
