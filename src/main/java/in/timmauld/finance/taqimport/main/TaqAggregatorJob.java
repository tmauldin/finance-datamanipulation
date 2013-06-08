package in.timmauld.finance.taqimport.main;

import in.timmauld.finance.taqimport.data.TaqDIModule;
import in.timmauld.finance.taqimport.data.TaqRepository;
import in.timmauld.finance.taqimport.data.TaqHbaseRepository.TaqHBaseTable;
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
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
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

import com.google.inject.Guice;
import com.google.inject.Injector;

public class TaqAggregatorJob extends Configured implements Tool{
	
	private final String TEMP_PATH = "temp";	
	private List<String> compressedFiles;
	private List<Path> aggregatePaths;

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
			System.err.println("Usage: TaqAggregatorJob <input path> <TAQ Table Name (TAQMinute, TAQThirtyMinute, TAQHour, TAQTwoHour, TAQHalfDay, TAQDay, TAQWeek, TAQMonth)>");
			System.exit(-1);
		}		
		
		Configuration conf = getConf();
		String tblName = args[1];
		String inputPath = args[0];
		conf.set(TaqRepository.TAQ_TABLE_CONFIG_NAME, tblName);
		
		// Inject repository dependency
		Injector injector = Guice.createInjector(new TaqDIModule());
		TaqRepository repository = injector.getInstance(TaqRepository.class);
		repository.setTableName(tblName);
		repository.createTableIfNotExists();		
		
		Path decompressedOutputPath = new Path(TEMP_PATH + "/decompressed");
		Path aggregateOutputPath = new Path(TEMP_PATH + "/aggregated");	
		compressedFiles = new ArrayList<String>();
		aggregatePaths = new ArrayList<Path>();		
		
		if (tblName.equals(TaqHBaseTable.TAQMinute.name())) {
			
			//If we're populating the initial TAQMinute table, we'll: 
			//			traverse the directory,
			//			extracting the data from the gzipped files,
			//			aggregate it by Minute
			//			calculate returns by minute
			//			and write it out to HBase
			traverseAndGetPaths(new File(inputPath));	
			
			for (String compressedFile : compressedFiles) {
				
				// Identity job to decompress the file
			    // This will only run on one node unfortunately, since gzipped files can't be split					
				Job decompressJob = JobBuilder.parseInputAndOutput(this, conf, 
						new String[] { new Path(compressedFile).toString(), decompressedOutputPath.toString() });
				if (decompressJob == null) {
				      return -1;
				}
				
				decompressJob.setJarByClass(TaqAggregatorJob.class);
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
			    aggregatePaths.add(aggregateOutputPath);
			    
			    Job aggregateJob = JobBuilder.parseInputAndOutput(this, conf, 
			    		new String[] { decompressedOutputPath.toString(), aggregateOutputPath.toString() });
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
			    FileUtil.fullyDelete(new File(decompressedOutputPath.toString()));
			}
			
			//********************************************************************************************
		    
			// Job to calculate returns at each time t
		    // Setting to one reducer to do an in memory sort by time for each security
		    // Writing out the results to HBase
		    String returnsOutputPath = TEMP_PATH + "/returns";	
		    Job returnsJob = new Job(conf);
		    
		    // Set up the table reducer job
		    TableMapReduceUtil.initTableReducerJob(
		    		conf.get(TaqRepository.TAQ_TABLE_CONFIG_NAME),        // output table
		    		TaqReturnsTableReducer.class,    // reducer class
		    		returnsJob);
		    
		    // Set up all the input paths
		    for (Path aggregatePath : aggregatePaths) {
		    	SequenceFileInputFormat.addInputPath(returnsJob, aggregatePath);
		    }
		    
		    returnsJob.setJarByClass(TaqAggregatorJob.class);
		    SequenceFileOutputFormat.setOutputPath(returnsJob, new Path(returnsOutputPath));
		    returnsJob.setInputFormatClass(SequenceFileInputFormat.class);
		    returnsJob.setMapperClass(TaqReturnsMapper.class);
		    returnsJob.setMapOutputKeyClass(Text.class);
		    returnsJob.setMapOutputValueClass(TaqAggregationWritable.class);
		    returnsJob.setNumReduceTasks(1);
		    returnsJob.waitForCompletion(true);
			
		    // To conserve file space
		    FileUtil.fullyDelete(new File(aggregateOutputPath.toString()));
		    return 0;
		}
		
		// TODO: other tables will aggregate and calculate returns starting from the minutes records
		return 0;
			
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