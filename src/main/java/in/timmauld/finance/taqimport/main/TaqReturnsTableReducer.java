package in.timmauld.finance.taqimport.main;

import in.timmauld.finance.taqimport.data.TaqHbaseRepository;
import in.timmauld.finance.taqimport.data.TaqRepository;
import in.timmauld.finance.taqimport.data.TaqHbaseRepository.TaqHBaseTable;
import in.timmauld.finance.taqimport.data.model.TaqAggregationWritable;
import in.timmauld.finance.taqimport.data.model.TaqReturnsWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

public class TaqReturnsTableReducer extends TableReducer<Text, TaqAggregationWritable, ImmutableBytesWritable>  {

	private TaqRepository repository;
	
	@Override
 	public void reduce(Text key, Iterable<TaqAggregationWritable> values, Context context) throws IOException, InterruptedException {
		
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
			Put putOut = repository.getTaqReturnsPut(currentTaq);
			context.write(null, putOut);
		}
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
