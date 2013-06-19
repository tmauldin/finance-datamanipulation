package in.timmauld.finance.taqimport.aggregator;

import in.timmauld.finance.taqimport.data.model.TaqAggregationWritable;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TaqReturnsMapper extends Mapper<Text, TaqAggregationWritable, Text, TaqAggregationWritable>{
	
	@Override 
	public void map(Text key, TaqAggregationWritable value, Context context) throws IOException, InterruptedException {
		context.write(new Text(value.getTicker()), value);		
	}
}
