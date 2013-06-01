package in.timmauld.finance.taqimport;

import in.timmauld.finance.taqimport.data.TaqAggregationWritable;
import in.timmauld.hadoop.util.JobBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;

public class TaqAggregatorJob extends Configured implements Tool{
	
	private static int fileNo = 0;
	private final String TEMP_PATH = "temp";	
	
	public int traverse(File node){	 
		
		if(node.isDirectory()){
			String[] subNote = node.list();
			for(String filename : subNote){
				traverse(new File(node, filename));
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
		    Job job = new Job(); 
		    SequenceFileInputFormat.addInputPath( job, new Path( decompressedFileOutput)); 
		    TableOutputFormat.setOutputPath( job, new Path( args[ 1])); 
		    job.setMapperClass( MaxTemperatureMapper.class); 
		    job.setReducerClass( MaxTemperatureReducer.class); 
		    job.setOutputKeyClass( Text.class); 
		    job.setOutputValueClass( IntWritable.class); 
		    System.exit( job.waitForCompletion( true) ? 0 : 1);

		    
		    
			JobConf taqAggregationConf = new JobConf(TaqAggregatorJob.class);		
			taqAggregationConf.setJobName("Aggregate gzip" + fileNo);
			FileInputFormat.addInputPath(taqAggregationConf, decompressedOutputPath);
			taqAggregationConf.setOutputKeyClass(Text.class);
			taqAggregationConf.setOutputValueClass(IntWritable.class);
			taqAggregationConf.setMapperClass(TaqAggregationMapper.class);
			Job aggregateJob = new Job(taqAggregationConf);
			TableMapReduceUtil.initTableReducerJob(
					targetTable,      // output table
					null,             // reducer class
					aggregateJob);
			aggregateJob.waitForCompletion(true);
			
			fileNo++;
		}
 
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: TaqAggregatorJob <input path>");
			System.exit(-1);
		}	
		
		traverse(new File(args[0]));		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TaqAggregatorJob(), args);
	    System.exit(exitCode);
		
	}
	
	
}
