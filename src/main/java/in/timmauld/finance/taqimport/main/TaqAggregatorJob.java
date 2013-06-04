package in.timmauld.finance.taqimport.main;

import in.timmauld.hadoop.util.JobBuilder;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TaqAggregatorJob extends Configured implements Tool{
	
	private static int fileNo = 0;
	private final String TEMP_PATH = "temp";	
	
	public int traverse(File node) throws IOException, ClassNotFoundException, InterruptedException{	 
		
		if(node.isDirectory()){
			String[] subNote = node.list();
			for(String filename : subNote){
				return traverse(new File(node, filename));
			}
		} else {
			
			// Identity job to decompress the file
		    // This will only run on one node unfortunately, since gzipped files can't be split	
			String decompressedFileOutput = TEMP_PATH + fileNo;
			Job decompressJob = JobBuilder.parseInputAndOutput(this, getConf(), new String[] { node.getPath(), new Path(decompressedFileOutput).toString() });
		    if (decompressJob == null) {
		      return -1;
		    }

		    decompressJob.setInputFormatClass(TextInputFormat.class);		    
		    decompressJob.setMapperClass(Mapper.class);		    
		    decompressJob.setMapOutputKeyClass(LongWritable.class);
		    decompressJob.setMapOutputValueClass(Text.class);
		    decompressJob.setPartitionerClass(HashPartitioner.class);
		    decompressJob.setNumReduceTasks(1);
		    decompressJob.setReducerClass(Reducer.class);
		    decompressJob.setOutputKeyClass(LongWritable.class);
		    decompressJob.setOutputValueClass(Text.class);
		    decompressJob.setOutputFormatClass(SequenceFileOutputFormat.class);   
		    decompressJob.waitForCompletion(true);
		    
		    //********************************************************************************************
		    
			// Aggregation job
		    // This will be properly distributed
		    Job job = new Job(); 
		    SequenceFileInputFormat.addInputPath(job, new Path(decompressedFileOutput)); 
		    job.setMapperClass(TaqAggregatorMapper.class); 
		    job.setReducerClass(TaqAggregatorTableReducer.class); 
		    job.setOutputKeyClass( Text.class); 
		    job.setOutputValueClass( IntWritable.class); 
		    job.waitForCompletion(true);
			
			fileNo++;
		}
		return 0;
	}

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
			System.err.println("Usage: TaqAggregatorJob <input path>");
			System.exit(-1);
		}		
		
		return traverse(new File(args[0]));		
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TaqAggregatorJob(), args);
	    System.exit(exitCode);		
	}
}
