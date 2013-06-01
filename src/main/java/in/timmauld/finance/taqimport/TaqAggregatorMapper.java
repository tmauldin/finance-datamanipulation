package in.timmauld.finance.taqimport;

import in.timmauld.finance.taqimport.data.TaqAggregationWritable;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TaqAggregatorMapper extends Mapper<Object, Text, Text, TaqAggregationWritable>{
	
	@Override 
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		
	}
}
