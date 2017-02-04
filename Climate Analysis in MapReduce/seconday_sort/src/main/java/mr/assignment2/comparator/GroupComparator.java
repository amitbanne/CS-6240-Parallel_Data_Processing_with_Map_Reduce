package mr.assignment2.comparator;

import mr.assignment2.model.StationKey;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// Group Comparator sorts records based on station id

public class GroupComparator extends WritableComparator {

	public GroupComparator() {
		super(StationKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		StationKey sKey1 = (StationKey) a;
		StationKey sKey2 = (StationKey) b;
		return sKey1.compareTo(sKey2);
	}
}
