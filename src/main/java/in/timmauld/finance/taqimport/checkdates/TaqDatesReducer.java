package in.timmauld.finance.taqimport.checkdates;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TaqDatesReducer extends Reducer<LongWritable, Text, NullWritable, Text> {

	private static DateFormat dateParser = new SimpleDateFormat("yyyyMMdd");
	
	@Override
	protected void reduce(LongWritable key,
	                        Iterable<Text> values,
	                        Context context) throws IOException, InterruptedException {
		long timeStamp = key.get() * 60 * 24 * 60 * 1000;
		Date d = new Date(timeStamp);
		String tradingDate = dateParser.format(d);
		context.write(NullWritable.get(), new Text(tradingDate));
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		dateParser.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
}
