package in.timmauld.finance.taqimport.main;

import in.timmauld.finance.taqimport.data.model.TaqAggregationWritable;
import in.timmauld.finance.taqimport.data.model.TaqWritable;
import in.timmauld.hadoop.util.JobBuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TaqAggregatorJob extends Configured implements Tool{
	
	private final String TEMP_PATH = "temp";	
	private final String DECOMPRESSED_PATH = TEMP_PATH + "/decompressed";
	private final String AGGREGATED_PATH = TEMP_PATH + "/aggregated";	
	private List<String> compressedFiles;
	private List<String> aggregatePaths;

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 1) {
			System.err.println("Usage: TaqAggregatorJob <input path>");// <TAQ Table Name (TAQMinute, TAQThirtyMinute, TAQHour, TAQTwoHour, TAQHalfDay, TAQDay, TAQWeek, TAQMonth)>");
			System.exit(-1);
		}		
		
		Configuration conf = getConf();
		String inputPath = args[0];				
		
		compressedFiles = new ArrayList<String>();				
		aggregatePaths = new ArrayList<String>();		
		int fileNo = 0;
	
		//If we're populating the initial TAQMinute table, we'll: 
		//			traverse the directory,
		//			extracting the data from the gzipped files,
		//			aggregate it by Minute
		//			calculate returns by minute
		//			and write it out 
		traverseAndGetPaths(new File(inputPath));			
		
		for (String compressedFile : compressedFiles) {
			
			// Identity job to decompress the file
		    // This will only run on one node unfortunately, since gzipped files can't be split					
			Job decompressJob = JobBuilder.buildDecompressJob(this, conf, compressedFile, DECOMPRESSED_PATH);
			if (decompressJob == null) {
			      return -1;
			}  
		    decompressJob.waitForCompletion(true);

		    
		    //********************************************************************************************
		    
			// Aggregation job
		    // This will be properly distributed			 
		    Path aggregateOutputPath = new Path(AGGREGATED_PATH + "/" + fileNo);
		    aggregatePaths.add(aggregateOutputPath.toString());
		    
		    Job aggregateJob = JobBuilder.parseInputAndOutput(this, conf, 
		    		new String[] { DECOMPRESSED_PATH, aggregateOutputPath.toString() });
		    if (aggregateJob == null) {
			      return -1;
			}
		    
		    aggregateJob.setJarByClass(TaqAggregatorJob.class);
		    aggregateJob.setInputFormatClass(SequenceFileInputFormat.class);
		    aggregateJob.setMapperClass(TaqAggregatorMapper.class);
		    aggregateJob.setMapOutputKeyClass(Text.class);
		    aggregateJob.setMapOutputValueClass(TaqWritable.class);
		    aggregateJob.setNumReduceTasks(1);
		    aggregateJob.setReducerClass(TaqAggregatorReducer.class);
		    aggregateJob.setOutputKeyClass(Text.class);
		    aggregateJob.setOutputValueClass(TaqAggregationWritable.class);
		    aggregateJob.setOutputFormatClass(SequenceFileOutputFormat.class);   
		    aggregateJob.waitForCompletion(true);	
		    
		    // To conserve file space
		    FileUtil.fullyDelete(new File(DECOMPRESSED_PATH));
		}
		
		//********************************************************************************************
	    
		// Job to calculate returns at each time t
	    // Setting to one reducer to do an in memory sort by time for each security
	    // Writing out the results to HBase
	    String returnsOutputPath = TEMP_PATH + "/minute";	

	    Job returnsJob = new Job(conf);
	    returnsJob.setJarByClass(TaqAggregatorJob.class);
    
	    // Set up all the input paths
	    for (String aggregatePath : aggregatePaths) {
	    	SequenceFileInputFormat.addInputPath(returnsJob, new Path(aggregatePath));
	    }
	    
	    returnsJob.setJarByClass(TaqAggregatorJob.class);
	    returnsJob.setInputFormatClass(SequenceFileInputFormat.class);
	    returnsJob.setMapperClass(TaqReturnsMapper.class);
	    returnsJob.setMapOutputKeyClass(Text.class);
	    returnsJob.setMapOutputValueClass(TaqAggregationWritable.class);

	    returnsJob.setReducerClass(TaqAggregatorReducer.class);
	    returnsJob.setOutputKeyClass(Text.class);
	    returnsJob.setOutputValueClass(Text.class);
	    TextOutputFormat.setOutputPath(returnsJob, new Path(returnsOutputPath));
	    returnsJob.setOutputFormatClass(TextOutputFormat.class);  
	    
	    // So that we can sort by time
	    returnsJob.setNumReduceTasks(1);
	    returnsJob.waitForCompletion(true);
		
	    // To conserve file space
	    FileUtil.fullyDelete(new File(AGGREGATED_PATH));
	    return 0;
		
		// TODO: aggregate and calculate returns starting from the minutes records
			
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TaqAggregatorJob(), args);
	    System.exit(exitCode);		
	}
	
	public void traverseAndGetPaths(File node) throws IOException, ClassNotFoundException, InterruptedException{	
		if(node.isDirectory()){
			String[] subNode = node.list();
			for(String filename : subNode){
				if (!filename.equals(".DS_Store")) {
					traverseAndGetPaths(new File(node, filename));
				}				
			}
		} else {
			compressedFiles.add(node.getPath());			
		}				
	}
}