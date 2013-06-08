package in.timmauld.finance.taqimport.main;

import in.timmauld.finance.taqimport.data.model.TaqWritable;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TaqAggregatorMapper extends Mapper<Object, Text, Text, TaqWritable>{
	
	@Override 
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		TaqWritable taq = new TaqWritable();
		StringTokenizer tknz = new StringTokenizer(value.toString(), ",");
		SimpleDateFormat dateParser = new SimpleDateFormat("yyyyddMM hh:mm:ss");
		String ticker = tknz.nextToken();
		if (!ticker.equalsIgnoreCase("SYMBOL")) {
			taq.setTicker(ticker);
			try {
				taq.setTime(dateParser.parse(tknz.nextToken() + " " + tknz.nextToken()));
			} catch (ParseException e) {
				e.printStackTrace();
			}
			taq.setPrice(Double.parseDouble(tknz.nextToken()));
			taq.setNumShares(Long.parseLong(tknz.nextToken()));
			tknz.nextToken();
			int corr = Integer.parseInt(tknz.nextToken());
			
			// If TAQ record CORR > 3, this is a cancelled trade
			if (corr <= 3) {
				Text outputKey = new Text(taq.getTicker() + "_" + taq.getTime());
				context.write(outputKey, taq);			
			}
		}
		
	}
}
