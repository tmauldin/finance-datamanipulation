package in.timmauld.finance.taqimport.checkdates;

import in.timmauld.hadoop.util.JobBuilder;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TaqCheckDatesDriver extends Configured implements Tool{
	
	private static final Log LOG = LogFactory.getLog(TaqCheckDatesDriver.class);
	
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		String compressedDir = args[0];
		String dateOutput = args[1];
		FileSystem fs = FileSystem.get(conf);
			
		if (fs.exists(new Path(dateOutput))) {
	    	FileUtil.fullyDelete(new File(dateOutput));
	    }
		
		Job datesJob = JobBuilder.buildBasicJob(this, conf, dateOutput, compressedDir);
	    if (datesJob == null) { 
		      return -1;
		}
		    
	    datesJob.setJarByClass(TaqCheckDatesDriver.class);
	    datesJob.setInputFormatClass(TextInputFormat.class);
	    datesJob.setMapperClass(TaqDatesMapper.class);
	    datesJob.setMapOutputKeyClass(LongWritable.class);
	    datesJob.setMapOutputValueClass(Text.class);
	    
	    datesJob.setReducerClass(TaqDatesReducer.class);		    
	    datesJob.setOutputKeyClass(LongWritable.class);
	    datesJob.setOutputValueClass(Text.class);
	    datesJob.setOutputFormatClass(TextOutputFormat.class);  
	    
	    datesJob.waitForCompletion(true);

		return 0;
	}

}
