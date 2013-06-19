package in.timmauld.finance.taqimport.aggregator;

import in.timmauld.finance.taqimport.data.model.TaqAggregationWritable;
import in.timmauld.finance.taqimport.data.model.TaqWritable;
import in.timmauld.finance.taqimport.data.model.time.DateBehavior;
import in.timmauld.finance.taqimport.data.model.time.SecondsBehavior;
import in.timmauld.hadoop.util.JobBuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class TaqAggregatorDriver extends Configured implements Tool{
	
	private static final String AGGREGATED_PATH = "output/aggregated";
	private static final String RETURNS_PATH = "output/aggregatedreturns";
	private static final Log LOG = LogFactory.getLog(TaqAggregatorDriver.class);
	private DateBehavior dateBehavior;

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = getConf();
		String compressedDir = args[0];
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fss = fs.listStatus(new Path(compressedDir));
		dateBehavior = new SecondsBehavior();
		
	    List<String> inputs = new ArrayList<String>();
		for (FileStatus status : fss) {
    	
		    Path path = status.getPath();
		    if (!path.getName().startsWith(".")) {  
		    	inputs.add(compressedDir + "/" + path.getName());
		    }
		}
		
		String aggregatedOutput = AGGREGATED_PATH + "/" + dateBehavior.getOutputPath();		
		if (fs.exists(new Path(aggregatedOutput))) {
	    	FileUtil.fullyDelete(new File(aggregatedOutput));
	    }
		
		Job aggregateJob = JobBuilder.buildBasicJob(this, conf, aggregatedOutput, inputs.toArray(new String[0]));
	    if (aggregateJob == null) { 
		      return -1;
		}
	    
	    aggregateJob.setJarByClass(TaqAggregatorDriver.class);
	    aggregateJob.setInputFormatClass(SequenceFileInputFormat.class);
	    aggregateJob.setMapperClass(TaqAggregatorMapper.class);
	    aggregateJob.setMapOutputKeyClass(Text.class);
	    aggregateJob.setMapOutputValueClass(TaqWritable.class);

	    aggregateJob.setReducerClass(TaqAggregatorReducer.class);
	    aggregateJob.setOutputKeyClass(Text.class);
	    aggregateJob.setOutputValueClass(TaqAggregationWritable.class);
	    aggregateJob.setOutputFormatClass(SequenceFileOutputFormat.class);   
	    
	    if (!aggregateJob.waitForCompletion(true)) {
	    	return -1;
	    }
	    
	    //********************************************************************************************
	    
	    String returnsOutput = RETURNS_PATH + "/" + dateBehavior.getOutputPath();
	    Job returnsJob = JobBuilder.buildBasicJob(this, conf, returnsOutput, aggregatedOutput);
		
	    returnsJob.setJarByClass(TaqAggregatorDriver.class);
	    returnsJob.setInputFormatClass(SequenceFileInputFormat.class);
	    returnsJob.setMapperClass(TaqReturnsMapper.class);
	    returnsJob.setMapOutputKeyClass(Text.class);
	    returnsJob.setMapOutputValueClass(TaqAggregationWritable.class);

	    returnsJob.setReducerClass(TaqAggregatorReducer.class);
	    returnsJob.setOutputKeyClass(NullWritable.class);
	    returnsJob.setOutputValueClass(Text.class);
	    returnsJob.setOutputFormatClass(TextOutputFormat.class);  
	    
	    // So that we can sort by time
	    returnsJob.setNumReduceTasks(1);
	    
	    if (!returnsJob.waitForCompletion(true)) {
	    	return -1;
	    }
	    
	    return 0;
	}
}