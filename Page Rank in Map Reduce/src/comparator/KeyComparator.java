package mr.page_rank.comparator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// This key comparator sorts records emitted by Mapper of Job-3
// in the decreasing order of keys i.e. page-rank values
public class KeyComparator extends WritableComparator {

	public KeyComparator(){
		super(DoubleWritable.class, true);
	}
	
	@Override
	public int compare(WritableComparable wc1, WritableComparable wc2){
		
		DoubleWritable pageRank1 = (DoubleWritable) wc1;
		DoubleWritable pageRank2 = (DoubleWritable) wc2;
		// sort in descending order of page-rank values
		return pageRank2.compareTo(pageRank1);
	}
	
}
