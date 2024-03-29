package in.timmauld.finance.taqimport.aggregator;

import in.timmauld.finance.taqimport.data.model.TaqAggregationWritable;
import in.timmauld.finance.taqimport.data.model.TaqWritable;
import in.timmauld.finance.taqimport.data.model.time.DateBehavior;
import in.timmauld.finance.taqimport.data.model.time.SecondsBehavior;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TaqAggregatorMapperReducerTest {
	
	static SimpleDateFormat dateParser = new SimpleDateFormat("yyyyMMdd hh:mm:ss");
		
	MapDriver<LongWritable, Text, Text, TaqWritable> mapDriver;
	ReduceDriver<Text, TaqWritable, Text, TaqAggregationWritable> reduceDriver;
	DateBehavior dateBehavior;
		  
	@Before
	public void setUp() throws IOException {
		TaqAggregatorMapper mapper = new TaqAggregatorMapper();
		TaqAggregatorReducer reducer = new TaqAggregatorReducer();
	    mapDriver = MapDriver.newMapDriver(mapper);
	    reduceDriver = ReduceDriver.newReduceDriver(reducer);
	    dateBehavior = new SecondsBehavior();
	    dateParser.setTimeZone(TimeZone.getTimeZone("UTC"));
	}

	@Test
	public void testMapper() throws ParseException, IOException {
		TaqWritable taq = new TaqWritable(dateBehavior);

		taq.setTicker("A");
		taq.setTime(dateParser.parse("20090112 8:08:59"));
		taq.setPrice(new BigDecimal(18.99));
		taq.setNumShares(100);
		
		mapDriver.withInput(new LongWritable(), new Text(
	        "A,20090112,8:08:59," + new BigDecimal(18.99).toString() + ",100,0,0,F,T"));	    
		List<Pair<Text, TaqWritable>> results = mapDriver.run();
	    Assert.assertEquals(results.get(0).getSecond(), taq);
	}

	@Test
	public void testReducer() throws ParseException, IOException {
	    List<TaqWritable> values = new ArrayList<TaqWritable>();
	    Text key = new Text("A_" + "1231747739");
	    
	    TaqWritable taq0 = new TaqWritable(dateBehavior);
		taq0.setTicker("A");
		taq0.setTime(dateParser.parse("20090112 8:08:59"));
		taq0.setPrice(new BigDecimal(18.98));
		taq0.setNumShares(100);
		
		TaqWritable taq1 = new TaqWritable(dateBehavior);
		taq1.setTicker("A");
		taq1.setTime(dateParser.parse("20090112 8:08:59"));
		taq1.setPrice(new BigDecimal(19.0));
		taq1.setNumShares(100);
		
		TaqWritable taq2 = new TaqWritable(dateBehavior);
		taq2.setTicker("A");
		taq2.setTime(dateParser.parse("20090112 8:08:59"));
		taq2.setPrice(new BigDecimal(18.99));
		taq2.setNumShares(100);
		
		values.add(taq0);
	    values.add(taq1);
	    values.add(taq2);
		
		double sumOfSquares = 0.0;
		for (int i = 0; i < values.size(); i++) {
			sumOfSquares += Math.pow(values.get(i).getPrice().subtract(new BigDecimal(18.99)).doubleValue(), 2);     
		}
		
	    TaqAggregationWritable taqAgg = new TaqAggregationWritable();
	    taqAgg.setKey(key.toString());
	    taqAgg.setTicker("A");
	    taqAgg.setTime(1231747739L);
	    taqAgg.setHighPrice(new BigDecimal(19.00));
	    taqAgg.setLowPrice(new BigDecimal(18.98));
	    taqAgg.setMeanPrice(new BigDecimal(18.99));
	    taqAgg.setNumShares(300);
	    taqAgg.setNumTrades(3);
	    taqAgg.setVariance(sumOfSquares / 2);

	    reduceDriver.withInput(key, values);
	    List<Pair<Text, TaqAggregationWritable>> results = reduceDriver.run();
	    Assert.assertEquals(results.get(0).getSecond(), taqAgg);
	}
 
}
