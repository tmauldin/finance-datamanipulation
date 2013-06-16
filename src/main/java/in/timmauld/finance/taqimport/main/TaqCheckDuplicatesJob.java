package in.timmauld.finance.taqimport.main;

import in.timmauld.finance.taqimport.data.model.TaqWritable;
import in.timmauld.hadoop.util.JobBuilder;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TaqCheckDuplicatesJob extends Configured implements Tool{

	private static final String DATES_PATH = "output/dates";	
	private static final Log LOG = LogFactory.getLog(TaqCheckDuplicatesJob.class);
	
	static class TaqDatesMapper extends Mapper<Object, Text, LongWritable, Text>{
		
		static SimpleDateFormat dateParser = new SimpleDateFormat("yyyyMMdd hh:mm:ss");
		
		@Override 
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				TaqWritable taq = new TaqWritable();
				StringTokenizer tknz = new StringTokenizer(value.toString(), ",");
				String ticker = tknz.nextToken();
				if (!ticker.equalsIgnoreCase("SYMBOL")) {
					taq.setTicker(ticker);
					taq.setTime(dateParser.parse(tknz.nextToken() + " " + tknz.nextToken()));
					context.write(new LongWritable(taq.getTimeInDaysSinceEpoch()), new Text(taq.getTicker()));		
				}
			} catch (ParseException e) {
				LOG.error("ParseException at text line: " + value);
				e.printStackTrace();
			} catch (IOException e) {
				LOG.error("IOException at text line: " + value);
				e.printStackTrace();
			}
		}
	}
	
	static class TaqDatesReducer extends Reducer<LongWritable, Text, NullWritable, Text> {

		@Override
		  protected void reduce(LongWritable key,
		                        Iterable<Text> values,
		                        Context context) throws IOException, InterruptedException {
			long timeStamp = key.get() * 60 * 24 * 60 * 1000;
			Date d = new Date(timeStamp);
			DateFormat df = new SimpleDateFormat("yyyyMMdd");
			String tradingDate = df.format(d);
			context.write(null, new Text(tradingDate));
		}
	}
	
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		String compressedDir = args[0];
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fss = fs.listStatus(new Path(compressedDir));
	    for (FileStatus status : fss) {
	    	
		    Path path = status.getPath();
		    if (!path.getName().startsWith(".")) {    
		        String dateOutput = DATES_PATH + "/" + path.getName().replace(".csv.gz", "").replace("/", "_");
			    
			    if (fs.exists(new Path(dateOutput))) {
			    	FileUtil.fullyDelete(new File(dateOutput));
			    }
			    
			    Job datesJob = JobBuilder.parseInputAndOutput(this, conf, 
			    		new String[] { compressedDir + "/" + path.getName(), dateOutput });
			    if (datesJob == null) { 
				      return -1;
				}
			    
			    datesJob.setJarByClass(TaqCheckDuplicatesJob.class);
			    datesJob.setInputFormatClass(TextInputFormat.class);
			    datesJob.setMapperClass(TaqDatesMapper.class);
			    datesJob.setMapOutputKeyClass(LongWritable.class);
			    datesJob.setMapOutputValueClass(Text.class);
			    
			    datesJob.setReducerClass(TaqDatesReducer.class);		    
			    datesJob.setOutputKeyClass(LongWritable.class);
			    datesJob.setOutputValueClass(Text.class);
			    datesJob.setOutputFormatClass(TextOutputFormat.class);  
			    
			    datesJob.waitForCompletion(true);
	    	}
	    }
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		LOG.info("Time Started: " + new Date());
		int exitCode = ToolRunner.run(new TaqCheckDuplicatesJob(), new String[] { args[0] });
		LOG.info("Time Finished: " + new Date());
		System.exit(exitCode);		
	}

}
