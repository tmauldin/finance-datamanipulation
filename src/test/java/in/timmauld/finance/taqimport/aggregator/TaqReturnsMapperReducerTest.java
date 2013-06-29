package in.timmauld.finance.taqimport.aggregator;

import in.timmauld.finance.taqimport.data.model.TaqAggregationWritable;
import in.timmauld.finance.taqimport.data.model.time.DateBehavior;
import in.timmauld.finance.taqimport.data.model.time.SecondsBehavior;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

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
		dateParser.setTimeZone(TimeZone.getTimeZone("UTC"));
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
	    taqAgg.setHighPrice(new BigDecimal(19.00));
	    taqAgg.setLowPrice(new BigDecimal(18.98));
	    taqAgg.setMeanPrice(new BigDecimal(18.99));
	    taqAgg.setNumShares(300);
	    taqAgg.setVariance(1.0);
		
		mapDriver.withInput(new Text("A"), taqAgg);	    
	    mapDriver.withOutput(new Text("A"), taqAgg);
		mapDriver.runTest();
	    //Assert.assertEquals(mapDriver.getExpectedOutputs().get(0), new Pair<Text, TaqAggregationWritable>(new Text("A"), taqAgg));
	}
	
	@Test
	public void testReducer() throws ParseException, IOException {
	    List<TaqAggregationWritable> values = new ArrayList<TaqAggregationWritable>();
	    List<Pair<Text, Text>> expected = new ArrayList<Pair<Text, Text>>();
	    Text key = new Text("A_" + "1231747739");
		
	    TaqAggregationWritable taqAgg0 = new TaqAggregationWritable();
	    taqAgg0.setKey(key.toString());
	    taqAgg0.setTicker("A");
	    taqAgg0.setTime(1231747739);
	    taqAgg0.setHighPrice(new BigDecimal(19.0));
	    taqAgg0.setLowPrice(new BigDecimal(18.98));
	    taqAgg0.setMeanPrice(new BigDecimal(18.99));
	    taqAgg0.setNumShares(300);
	    taqAgg0.setNumTrades(1);
	    taqAgg0.setVariance(1.0);
	    
	    TaqAggregationWritable taqAgg1 = new TaqAggregationWritable();
	    taqAgg1.setKey("A_" + "1231747799");
	    taqAgg1.setTicker("A");
	    taqAgg1.setTime(1231747799);
	    taqAgg1.setHighPrice(new BigDecimal(19.01));
	    taqAgg1.setLowPrice(new BigDecimal(19.0));
	    taqAgg1.setMeanPrice(new BigDecimal(19.02));
	    taqAgg1.setNumShares(300);
	    taqAgg1.setNumTrades(2);
	    taqAgg1.setVariance(1.0);
	    
	    values.add(taqAgg0);
	    values.add(taqAgg1);
	    
	    StringBuilder expectedBuilder1 = new StringBuilder();
		expectedBuilder1.append("1231747739,");
		expectedBuilder1.append("A,");
		expectedBuilder1.append(",");
		expectedBuilder1.append(new BigDecimal(19.0) + ",");
		expectedBuilder1.append(new BigDecimal(18.99) + ",");
		expectedBuilder1.append(new BigDecimal(18.98) + ",");
		expectedBuilder1.append(new BigDecimal(0.0) + ",");
		expectedBuilder1.append(new BigDecimal(0.0) + ",");
		expectedBuilder1.append(new BigDecimal(0.0) + ",");
		expectedBuilder1.append("300,");
		expectedBuilder1.append("1,");
		expectedBuilder1.append("1.0");		
		Text expectedVal1 = new Text(expectedBuilder1.toString());
		expected.add(new Pair<Text, Text>(new Text("A_" + "1231747739"), expectedVal1));
		
		StringBuilder expectedBuilder2 = new StringBuilder();
		expectedBuilder2.append("1231747799,");
		expectedBuilder2.append("A,");
		expectedBuilder2.append(",");
		expectedBuilder2.append(new BigDecimal(19.01) + ",");
		expectedBuilder2.append(new BigDecimal(19.02) + ",");
		expectedBuilder2.append(new BigDecimal(19.0) + ",");
		expectedBuilder2.append(new BigDecimal(0.01).divide(new BigDecimal(19.0), 2, RoundingMode.HALF_UP) + ",");
		expectedBuilder2.append(new BigDecimal(0.02).divide(new BigDecimal(18.98), 2, RoundingMode.HALF_UP) + ",");
		expectedBuilder2.append(new BigDecimal(0.03).divide(new BigDecimal(18.99), 2, RoundingMode.HALF_UP) + ",");
		expectedBuilder2.append("300,");
		expectedBuilder2.append("2,");
		expectedBuilder2.append("1.0");		
		Text expectedVal2 = new Text(expectedBuilder2.toString());
		expected.add(new Pair<Text, Text>(new Text("A_" + "1231747799"), expectedVal2));
		
	    reduceDriver.withInput(key, values);
	    reduceDriver.withAllOutput(expected);
	    reduceDriver.run(true);
	    //Assert.assertEquals(reduceDriver.getExpectedOutputs().get(0), expected.get(0));
	    //Assert.assertEquals(reduceDriver.getExpectedOutputs().get(1), expected.get(1));
	}
 
}
