package in.timmauld.finance.taqimport.checkdates;

import in.timmauld.finance.taqimport.checkdates.TaqDatesMapper;
import in.timmauld.finance.taqimport.checkdates.TaqDatesReducer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TaqDatesMapperReducerTest {
	
	MapDriver<Object, Text, LongWritable, Text> mapDriver;
	ReduceDriver<LongWritable, Text, NullWritable, Text> reduceDriver;
	
	  
	@Before
	  public void setUp() {
		TaqDatesMapper mapper = new TaqDatesMapper();
		TaqDatesReducer reducer = new TaqDatesReducer();
	    mapDriver = MapDriver.newMapDriver(mapper);
	    reduceDriver = ReduceDriver.newReduceDriver(reducer);
	  }

	  @Test
	  public void testMapper() {
	    mapDriver.withInput(new Object(), new Text(
	        "A,20090112,8:08:59,18.99,100,0,0,F,T"));
	    mapDriver.withOutput(new LongWritable(14256), new Text("A"));
	    mapDriver.runTest();
	  }

	  @Test
	  public void testReducer() {
	    List<Text> values = new ArrayList<Text>();
	    values.add(new Text("A"));
	    values.add(new Text("A"));
	    reduceDriver.withInput(new LongWritable(14256), values);
	    reduceDriver.withOutput(NullWritable.get(), new Text("20090112"));
	    reduceDriver.runTest();
	  }
 
}
