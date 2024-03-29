package in.timmauld.finance.taqimport.checkdates;

import in.timmauld.finance.taqimport.data.model.TaqWritable;
import in.timmauld.finance.taqimport.data.model.time.DateBehavior;
import in.timmauld.finance.taqimport.data.model.time.DaysBehavior;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;
import java.util.TimeZone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TaqDatesMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
	
	private static DateFormat dateParser = new SimpleDateFormat("yyyyMMdd hh:mm:ss");
	private static final Log LOG = LogFactory.getLog(TaqDatesMapper.class);
	private DateBehavior dateBehavior;
	
	@Override 
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		String filename = fileSplit.getPath().getName();
		try {
			TaqWritable taq = new TaqWritable(dateBehavior);
			StringTokenizer tknz = new StringTokenizer(value.toString(), ",");
			String ticker = tknz.nextToken();
			if (!ticker.equalsIgnoreCase("SYMBOL")) {
				taq.setTicker(ticker);
				taq.setTime(dateParser.parse(tknz.nextToken() + " " + tknz.nextToken()));
				context.write(new LongWritable(taq.getTime()), new Text(taq.getTicker() + "," + filename));		
			}
		} catch (ParseException e) {
			LOG.error("ParseException at text line: " + value + " in file: " + filename);
			e.printStackTrace();
		} catch (IOException e) {
			LOG.error("IOException at text line: " + value + " in file: " + filename);
			e.printStackTrace();
		}
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		dateBehavior = new DaysBehavior();
		dateParser.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
}
