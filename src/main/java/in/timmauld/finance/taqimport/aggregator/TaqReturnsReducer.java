package in.timmauld.finance.taqimport.aggregator;

import in.timmauld.finance.taqimport.data.model.TaqAggregationWritable;
import in.timmauld.finance.taqimport.data.model.TaqReturnsWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TaqReturnsReducer extends Reducer<Text, TaqAggregationWritable, Text, Text> {

	@Override
	  protected void reduce(Text key, Iterable<TaqAggregationWritable> values,
	                        Context context) throws IOException, InterruptedException {
		// Since we're only going as granular as seconds, we can do this in memory
		List<TaqReturnsWritable> taqs = new ArrayList<TaqReturnsWritable>();
		
		for (TaqAggregationWritable val : values) {
			TaqReturnsWritable taq = new TaqReturnsWritable();
			taq.setKey(val.getKey());
			taq.setTime(val.getTime());
			taq.setTicker(val.getTicker());
			taq.setName(val.getName());
			taq.setHighPrice(val.getHighPrice());
			taq.setLowPrice(val.getLowPrice());
			taq.setMeanPrice(val.getMeanPrice());
			taq.setNumShares(val.getNumShares());
			taq.setNumTrades(val.getNumTrades());
			taq.setVariance(val.getVariance());
			taqs.add(taq);
		}
		
		// Sort it (crossing fingers to not run out of memory
		Collections.sort(taqs);
		
		// Calculate returns
		for (int i = 0; i < taqs.size(); i++) {
			
			TaqReturnsWritable currentTaq = taqs.get(i);
			currentTaq.setHighPercentChange(0.0);
			currentTaq.setLowPercentChange(0.0);
			currentTaq.setMeanPercentChange(0.0);
			
			if ( i > 0) {
				TaqReturnsWritable previousTaq = taqs.get(i - 1);				
				
				// Checking for zeroes just in case
				if (previousTaq.getHighPrice().equals(new Double(0.0))) {
					currentTaq.setHighPercentChange((currentTaq.getHighPrice() - previousTaq.getHighPrice()) / previousTaq.getHighPrice());
				}
				if (previousTaq.getLowPrice().equals(new Double(0.0))) {
					currentTaq.setLowPercentChange((currentTaq.getLowPrice() - previousTaq.getLowPrice()) / previousTaq.getLowPrice());
				}
				if (previousTaq.getMeanPrice().equals(new Double(0.0))) {
					currentTaq.setMeanPercentChange((currentTaq.getMeanPrice() - previousTaq.getMeanPrice()) / previousTaq.getMeanPrice());
				}								
			} 		
			
			// Write it out
			StringBuilder toWriteBldr = new StringBuilder();
			toWriteBldr.append(currentTaq.getKey() + ",");
			toWriteBldr.append(currentTaq.getTime() + ",");
			toWriteBldr.append(currentTaq.getTicker() + ",");
			toWriteBldr.append(currentTaq.getName() + ",");
			toWriteBldr.append(currentTaq.getHighPrice() + ",");
			toWriteBldr.append(currentTaq.getMeanPrice() + ",");
			toWriteBldr.append(currentTaq.getLowPrice() + ",");
			toWriteBldr.append(currentTaq.getHighPercentChange() + ",");
			toWriteBldr.append(currentTaq.getLowPercentChange() + ",");
			toWriteBldr.append(currentTaq.getMeanPercentChange() + ",");
			toWriteBldr.append(currentTaq.getNumShares() + ",");
			toWriteBldr.append(currentTaq.getNumTrades() + ",");
			toWriteBldr.append(currentTaq.getVariance() + ",");
			
			context.write(new Text(currentTaq.getKey()), new Text(toWriteBldr.toString()));
		}
	}

}
