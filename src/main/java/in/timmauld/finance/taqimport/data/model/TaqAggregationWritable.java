package in.timmauld.finance.taqimport.data.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class TaqAggregationWritable implements Writable{	
	
	private String key;
	private long time;
	private String ticker;
	private String name = "";
	private Double highPrice;
	private Double lowPrice;
	private Double meanPrice;
	private long numShares;
	private long numTrades;	
	private Double variance;

	public TaqAggregationWritable() {
	}
	
	public String getKey() {
		return key;
	}
	
	public void setKey(String key) {
		this.key = key;
	}
	
	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}
	
	public String getTicker() {
		return ticker;
	}
	
	public void setTicker(String ticker) {		
		this.ticker = ticker;
	}		
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}

	public Double getHighPrice() {
		return highPrice;
	}

	public void setHighPrice(Double high) {
		this.highPrice = high;
	}

	public Double getLowPrice() {
		return lowPrice;
	}

	public void setLowPrice(Double low) {
		this.lowPrice = low;
	}

	public Double getMeanPrice() {
		return meanPrice;
	}

	public void setMeanPrice(Double mean) {
		this.meanPrice = mean;
	}

	public long getNumShares() {
		return numShares;
	}

	public void setNumShares(long numShares) {
		this.numShares = numShares;
	}

	public long getNumTrades() {
		return numTrades;
	}

	public void setNumTrades(long numTrades) {
		this.numTrades = numTrades;
	}
	
	public double getVariance() {
		return variance;
	}

	public void setVariance(Double variance) {
		this.variance = variance;
	}

	public void readFields(DataInput in) throws IOException {
		key = in.readUTF();
		time = in.readLong();
		ticker = in.readUTF();
		name = in.readUTF();
		highPrice = in.readDouble();
		lowPrice = in.readDouble();
		meanPrice = in.readDouble();
		numShares = in.readLong();
		numTrades = in.readLong();
		variance = in.readDouble();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(key);
		out.writeLong(time);
		out.writeUTF(ticker);
		out.writeUTF(name);
		out.writeDouble(highPrice);
		out.writeDouble(lowPrice);
		out.writeDouble(meanPrice);
		out.writeLong(numShares);
		out.writeLong(numTrades);
		out.writeDouble(variance);
	}
	
}
