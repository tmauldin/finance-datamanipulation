package in.timmauld.finance.taqimport.aggregator;

import in.timmauld.finance.taqimport.data.model.TaqWritable;
import in.timmauld.finance.taqimport.data.model.time.DateBehavior;
import in.timmauld.finance.taqimport.data.model.time.SecondsBehavior;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;
import java.util.TimeZone;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TaqAggregatorMapper extends Mapper<LongWritable, Text, Text, TaqWritable>{
	
	static SimpleDateFormat dateParser = new SimpleDateFormat("yyyyMMdd hh:mm:ss");
	private DateBehavior dateBehavior;
	
	@Override 
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		TaqWritable taq = new TaqWritable(dateBehavior);
		
		StringTokenizer tknz = new StringTokenizer(value.toString(), ",");
		String ticker = tknz.nextToken();
		if (!ticker.equalsIgnoreCase("SYMBOL")) {
			taq.setTicker(ticker);
			try {
				taq.setTime(dateParser.parse(tknz.nextToken() + " " + tknz.nextToken()));
			} catch (ParseException e) {
				e.printStackTrace();
			}
			taq.setPrice(new BigDecimal(tknz.nextToken()));
			taq.setNumShares(Long.parseLong(tknz.nextToken()));
			tknz.nextToken();
			int corr = Integer.parseInt(tknz.nextToken());
			
			// If TAQ record CORR > 3, this is a cancelled trade
			Text outputKey = new Text(taq.getTicker() + "_" + taq.getTime());
			if (corr <= 3) {				
				context.write(outputKey, taq);			
			}
		}		
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		dateBehavior = new SecondsBehavior();
		dateParser.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
}
