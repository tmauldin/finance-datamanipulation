package in.timmauld.finance.taqimport.main;

import in.timmauld.finance.taqimport.aggregator.TaqAggregatorDriver;
import in.timmauld.finance.taqimport.checkdates.TaqCheckDatesDriver;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ToolRunner;

public class TaqImportMain {
	
	private static final Log LOG = LogFactory.getLog(TaqCheckDatesDriver.class);
	
	public static void main(String[] args) throws Exception {
		int exitCode = 0;	
		
		if (args.length != 2) {
			System.err.printf("Usage: <action [checkdates | aggregate]> <output>");
			exitCode = -1;
			System.exit(exitCode);	
		}
		
		LOG.info("Time Started: " + new Date());
		
		if (args[0] == "checkdates") {
			exitCode = ToolRunner.run(new TaqCheckDatesDriver(), new String[] { args[1] });
		} else if (args[0] == "aggregate") {
			exitCode = ToolRunner.run(new TaqAggregatorDriver(), new String[] { args[1] });
		}
		
		LOG.info("Time Finished: " + new Date());
		System.exit(exitCode);		
	}
	
}
