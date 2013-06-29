package in.timmauld.finance.taqimport.aggregator;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import in.timmauld.finance.taqimport.data.model.TaqAggregationWritable;
import in.timmauld.finance.taqimport.data.model.TaqWritable;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TaqAggregatorReducer extends Reducer<Text, TaqWritable, Text, TaqAggregationWritable> {

	@Override
	  protected void reduce(Text key,
	                        Iterable<TaqWritable> values,
	                        Context context) throws IOException, InterruptedException {
		String ticker = "";
		long time = 0;
		List<Double> prices = new ArrayList<Double>();
		BigDecimal highPrice = new BigDecimal(0.0);
		BigDecimal lowPrice = new BigDecimal(Double.MAX_VALUE);
 		long numTrades = 0;
 		long numSharesTraded = 0;
		
 		for (TaqWritable val : values) {
 			ticker = val.getTicker();
 			time = val.getTime();
			prices.add(val.getPrice().doubleValue());
			if (val.getPrice().compareTo(highPrice) > 0) {
				highPrice = val.getPrice();
			}
			if (val.getPrice().compareTo(lowPrice) < 0) {
				lowPrice = val.getPrice();
			}
			numTrades++;
			numSharesTraded += val.getNumShares();
		}
		
 		Collections.sort(prices);
		double[] dblPrices = new double[prices.size()];
		for (int i = 0; i < prices.size(); i++) {
		    dblPrices[i] = prices.get(i);     
		}
		 
		DescriptiveStatistics stats = new DescriptiveStatistics(dblPrices); 
		double mean = stats.getMean();
		
		double sumOfSquares = 0.0;
		for (int i = 0; i < prices.size(); i++) {
			sumOfSquares += Math.pow((prices.get(i) - mean), 2);     
		}
		
		// Create new TaqAggregationWritable
		TaqAggregationWritable result = new TaqAggregationWritable();
		result.setKey(key.toString());
		result.setTime(time);
		result.setTicker(ticker);
		result.setHighPrice(highPrice);
		result.setLowPrice(lowPrice);
		result.setMeanPrice(new BigDecimal(mean));
		result.setNumShares(numSharesTraded);
		result.setNumTrades(numTrades);
		
		if (prices.size() > 1) {
			result.setVariance(sumOfSquares/(prices.size() - 1));
		} else {
			result.setVariance(0.0);
		}		
		
		// Write it out
		context.write(key, result);
	  }
	
}
