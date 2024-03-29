package in.timmauld.finance.taqimport.data.model;

import in.timmauld.finance.taqimport.data.model.time.DateBehavior;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.io.Writable;

public class TaqWritable implements Writable{

	private long time = 0;
	private String ticker = "";
	private BigDecimal price = new BigDecimal(0);
	private long numShares = 0;
	private DateBehavior dateBehavior; 
	
	public TaqWritable(){}
	
	public TaqWritable(DateBehavior dateBehavior) {
		this.dateBehavior = dateBehavior;
	}
	
	public long getTime() {
		return time;
	}

	public void setTime(Date time) {
		this.time = dateBehavior.getEpochTime(time);
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

	public BigDecimal getPrice() {
		return price;
	}

	public void setPrice(BigDecimal high) {
		this.price = high;
	}
	
	public long getNumShares() {
		return numShares;
	}

	public void setNumShares(long numShares) {
		this.numShares = numShares;
	}

	public void readFields(DataInput in) throws IOException {
		time = in.readLong();
		ticker = in.readUTF();
		price = new BigDecimal(in.readUTF());
		numShares = in.readLong();
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(time);
		out.writeUTF(ticker);
		out.writeUTF(price.toString());
		out.writeLong(numShares);
	}
	
	@Override
	public boolean equals(Object obj) {
		TaqWritable taqToCompare = (TaqWritable)obj;
		if (this.getNumShares() != taqToCompare.getNumShares()) {
			return false;
		}
		if (this.getTime() != taqToCompare.getTime()) {
			return false;
		}
		if (!this.getPrice().equals(taqToCompare.getPrice())) {
			return false;
		}
		if (!this.getTicker().equals(taqToCompare.getTicker())) {
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
