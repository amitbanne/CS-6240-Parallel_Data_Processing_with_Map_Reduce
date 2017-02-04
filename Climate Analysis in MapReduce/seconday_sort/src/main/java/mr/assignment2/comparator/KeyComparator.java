package mr.assignment2.comparator;

import mr.assignment2.model.StationKey;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


// Key Comparator sorts records based on station id and year
// i.e. sort records by station id, and if station ids are the same,
// sort by year

public class KeyComparator extends WritableComparator {
	
	public KeyComparator(){
		super(StationKey.class, true);
	}
	
	@Override
	public int compare(WritableComparable wc1, WritableComparable wc2){
		
		StationKey sKey1 = (StationKey) wc1;
		StationKey sKey2 = (StationKey) wc2;
		
		if(sKey1.getStationId().compareTo(sKey2.getStationId()) ==0)
			return sKey1.getYear().compareTo(sKey2.getYear());
		else
			return sKey1.getStationId().compareTo(sKey2.getStationId());
	}

}
