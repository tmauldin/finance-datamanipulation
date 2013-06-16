package in.timmauld.finance.taqimport.main;

import in.timmauld.finance.taqimport.data.model.TaqWritable;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

public class TaqAggregatorMapperTest {
	
	@Test 
	public void processesValidRecord() throws IOException, InterruptedException, ParseException {
//		SimpleDateFormat dateParser = new SimpleDateFormat("yyyyddMM hh:mm:ss");
//		Text value = new Text("A,20090112,8:08:59,18.99,100,0,0,F,T"); 
//		TaqWritable expected = new TaqWritable();
//		expected.setTicker("A");
//		expected.setTime(dateParser.parse("20090112" + " " + "8:08:59"));
//		expected.setPrice(18.99);
//		expected.setNumShares(100);
//		new MapDriver<Object, Text, Text, TaqWritable>()
//			.withMapper( new TaqAggregatorMapper()) 
//			.withInputValue( value) 
//			.withOutput( new Text("A_20090112"), expected) 
//			.runTest(); 

	}
 
}
