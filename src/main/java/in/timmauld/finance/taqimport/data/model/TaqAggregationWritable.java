package in.timmauld.finance.taqimport.data.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class TaqAggregationWritable implements Writable, WritableComparable<TaqAggregationWritable>, Comparable<TaqAggregationWritable>{	
	
	private String key;
	private long time;
	private String ticker;
	private String name = "";
	private BigDecimal highPrice;
	private BigDecimal lowPrice;
	private BigDecimal meanPrice;
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

	public BigDecimal getHighPrice() {
		return highPrice;
	}

	public void setHighPrice(BigDecimal high) {
		this.highPrice = high;
	}

	public BigDecimal getLowPrice() {
		return lowPrice;
	}

	public void setLowPrice(BigDecimal low) {
		this.lowPrice = low;
	}

	public BigDecimal getMeanPrice() {
		return meanPrice;
	}

	public void setMeanPrice(BigDecimal mean) {
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
		highPrice = new BigDecimal(in.readUTF());
		lowPrice = new BigDecimal(in.readUTF());
		meanPrice = new BigDecimal(in.readUTF());
		numShares = in.readLong();
		numTrades = in.readLong();
		variance = in.readDouble();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(key);
		out.writeLong(time);
		out.writeUTF(ticker);
		out.writeUTF(name);
		out.writeUTF(highPrice.toString());
		out.writeUTF(lowPrice.toString());
		out.writeUTF(meanPrice.toString());
		out.writeLong(numShares);
		out.writeLong(numTrades);
		out.writeDouble(variance);
	}

	@Override
	public int compareTo(TaqAggregationWritable taq) {
		final int BEFORE = -1;
		final int EQUAL = 0;
		final int AFTER = 1;

		if (this == taq) {
			return EQUAL;
		}
		if (this.getTime() < taq.getTime()) {
			return BEFORE;
		} else if (this.getTime() > taq.getTime()) {
			return AFTER;
		}
		return EQUAL;
	}
	
	@Override
	public boolean equals(Object obj) {
		TaqAggregationWritable taqToCompare = (TaqAggregationWritable)obj;
		if (this.getNumShares() != taqToCompare.getNumShares()) {
			return false;
		}
		if (this.getNumTrades() != taqToCompare.getNumTrades()) {
			return false;
		}
		if (this.getTime() != taqToCompare.getTime()) {
			return false;
		}
		if (!this.getTicker().equals(taqToCompare.getTicker())) {
			return false;
		}
		if (!this.getLowPrice().equals(taqToCompare.getLowPrice())) {
			return false;
		}
		if (!this.getHighPrice().equals(taqToCompare.getHighPrice())) {
			return false;
		}
		if (!this.getMeanPrice().equals(taqToCompare.getMeanPrice())) {
			return false;
		}
		
		return true;			
	}
	
	@Override
	public int hashCode() {
		return (int)getTime();
	}
	
	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}
}
