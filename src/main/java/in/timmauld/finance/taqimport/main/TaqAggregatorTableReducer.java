package in.timmauld.finance.taqimport.main;

import in.timmauld.finance.taqimport.data.TaqHbaseRepository;
import in.timmauld.finance.taqimport.data.TaqRepository;
import in.timmauld.finance.taqimport.data.TaqHbaseRepository.TaqHBaseTable;
import in.timmauld.finance.taqimport.data.model.TaqAggregationWritable;
import in.timmauld.finance.taqimport.data.model.TaqWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

public class TaqAggregatorTableReducer extends TableReducer<Text, TaqWritable, ImmutableBytesWritable>  {

	private TaqRepository repository;
	
	@Override
 	public void reduce(Text key, Iterable<TaqWritable> values, Context context) throws IOException, InterruptedException {

		// Aggregate price and volume
		String ticker = "";
		List<Double> prices = new ArrayList<Double>();
		double highPrice = 0.0;
 		double lowPrice = Double.MAX_VALUE;
 		long numTrades = 0;
 		long numSharesTraded = 0;
		
 		for (TaqWritable val : values) {
 			ticker = val.getTicker();
			prices.add(val.getPrice());
			if (val.getPrice() > highPrice) {
				highPrice = val.getPrice();
			}
			if (val.getPrice() < lowPrice) {
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
		result.setTicker(ticker);
		result.setHighPrice(highPrice);
		result.setLowPrice(lowPrice);
		result.setMeanPrice(mean);
		result.setNumShares(numSharesTraded);
		result.setNumTrades(numTrades);
		result.setVariance(sumOfSquares/(prices.size() - 1));
		
		// Write it out
		Put putOut = repository.getTaqAggregationPut(result);
		context.write(null, putOut);	
   	}
	
 	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{
		super.cleanup(context);
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		// TODO put DI back in
		TaqRepository repository = new TaqHbaseRepository(TaqHBaseTable.TAQMinute);
		Configuration conf = context.getConfiguration();
		String tableName = conf.get(TaqRepository.TAQ_TABLE_CONFIG_NAME);
		repository.setTableName(tableName);
		repository.createTableIfNotExists();
	}
}
