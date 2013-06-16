package in.timmauld.finance.taqimport.data.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.Writable;

public class TaqWritable implements Writable{

	private long time = 0;
	private String ticker = "";
	private Double price = 0.0;
	private long numShares = 0;
	
	public TaqWritable() {
	}
	
	public long getTime() {
		return time;
	}
	
	public long getTimeInDaysSinceEpoch() {
		return time / (60 * 24);
	}

	public void setTime(Date time) {
		long minutesSinceEpoch = time.getTime() / (60 * 1000);
		this.time = minutesSinceEpoch;
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

	public Double getPrice() {
		return price;
	}

	public void setPrice(Double high) {
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
		price = in.readDouble();
		numShares = in.readLong();
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(time);
		out.writeUTF(ticker);
		out.writeDouble(price);
		out.writeLong(numShares);
	}
}
