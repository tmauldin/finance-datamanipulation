package in.timmauld.finance.taqimport.aggregator;

import in.timmauld.finance.taqimport.data.model.TaqAggregationWritable;
import in.timmauld.finance.taqimport.data.model.TaqReturnsWritable;
import in.timmauld.finance.taqimport.data.model.TaqWritable;
import in.timmauld.finance.taqimport.data.model.time.DateBehavior;
import in.timmauld.finance.taqimport.data.model.time.SecondsBehavior;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TaqReturnsMapperReducerTest {
	
	static SimpleDateFormat dateParser = new SimpleDateFormat("yyyyMMdd hh:mm:ss");
		
	MapDriver<Text, TaqAggregationWritable, Text, TaqAggregationWritable> mapDriver;
	ReduceDriver<Text, TaqAggregationWritable, Text, Text> reduceDriver;
	DateBehavior dateBehavior;
		  
	@Before
	public void setUp() throws IOException {
		TaqReturnsMapper mapper = new TaqReturnsMapper();
		TaqReturnsReducer reducer = new TaqReturnsReducer();
	    mapDriver = MapDriver.newMapDriver(mapper);
	    reduceDriver = ReduceDriver.newReduceDriver(reducer);
	    dateBehavior = new SecondsBehavior();
	}

	@Test
	public void testMapper() throws ParseException, IOException {
		TaqAggregationWritable taqAgg = new TaqAggregationWritable();
		taqAgg.setKey("A");
		taqAgg.setTicker("A");
	    taqAgg.setTime(20090112L);
	    taqAgg.setHighPrice(19.00);
	    taqAgg.setLowPrice(18.98);
	    taqAgg.setMeanPrice(18.99);
	    taqAgg.setNumShares(300);
	    taqAgg.setVariance(1.0);
		
		mapDriver.withInput(new Text("A"), taqAgg);	    
	    mapDriver.withOutput(new Text("A"), taqAgg);
		mapDriver.run();
	    Assert.assertEquals(mapDriver.getExpectedOutputs().get(0), new Pair<Text, TaqAggregationWritable>(new Text("A"), taqAgg));
	}
	
	//TODO reducer test
 
}
