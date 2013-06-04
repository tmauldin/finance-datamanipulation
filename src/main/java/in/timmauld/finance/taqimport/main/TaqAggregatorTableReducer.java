package in.timmauld.finance.taqimport.main;

import in.timmauld.finance.taqimport.data.TaqDIModule;
import in.timmauld.finance.taqimport.data.TaqHbaseRepository.TaqHBaseTable;
import in.timmauld.finance.taqimport.data.TaqRepository;
import in.timmauld.finance.taqimport.data.TaqAggregationWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class TaqAggregatorTableReducer extends TableReducer<Text, TaqAggregationWritable, ImmutableBytesWritable>  {

	private TaqRepository repository;
	
	@Override
 	public void reduce(Text key, Iterable<TaqAggregationWritable> values, Context context) throws IOException, InterruptedException {
		
		// Aggregate price and volume
		String ticker = "";
		String name = "";
		List<Double> prices = new ArrayList<Double>();
		double highPrice = 0.0;
 		double lowPrice = 0.0;
 		long numTrades = 0;
 		long numSharesTraded = 0;
		
 		for (TaqAggregationWritable val : values) {
 			ticker = val.getTicker();
 			name = val.getName();
			prices.add(val.getMeanPrice());
			if (val.getHighPrice() > highPrice) {
				highPrice = val.getHighPrice();
			}
			if (val.getLowPrice() < lowPrice) {
				lowPrice = val.getLowPrice();
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
		result.setTicker(ticker);
		result.setName(name);
		result.setHighPrice(highPrice);
		result.setLowPrice(lowPrice);
		result.setMeanPrice(mean);
		result.setNumShares(numSharesTraded);
		result.setNumTrades(numTrades);
		result.setVariance(sumOfSquares/(prices.size() - 1));
		
		// Write it out
		repository.addTaqRow(result);
		
   	}
	
 	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{
		super.cleanup(context);
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		// Inject repository dependency
		Injector injector = Guice.createInjector(new TaqDIModule());
		repository = injector.getInstance(TaqRepository.class);
		repository.createTableIfNotExists();
	}
}
