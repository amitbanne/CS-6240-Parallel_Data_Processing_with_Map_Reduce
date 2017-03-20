package mr.page_rank.partitioner;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * ResultPartitioner ensures that all the data is 
 * sent to the only one reducer we have for JOB-2
 **/

public class ResultPartitioner extends Partitioner<DoubleWritable, Text> implements Configurable {

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setConf(Configuration arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getPartition(DoubleWritable arg0, Text arg1, int arg2) {
		// TODO Auto-generated method stub
		return 0;
	}

}
