package in.timmauld.finance.taqimport.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.Writable;

public class TaqAggregationWritable implements Writable{	
	
	private Date time;
	private String ticker;
	private String name;
	private Double highPrice;
	private Double lowPrice;
	private Double medPrice;
	private long numShares;
	private long numTrades;	
	private Double highPercentChange;
	private Double lowPercentChange;
	private Double medPercentChange;

	public TaqAggregationWritable() {
	}
	
	public Date getTime() {
		return time;
	}

	public void setTime(Date time) {
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

	public Double getMedPrice() {
		return medPrice;
	}

	public void setMedPrice(Double med) {
		this.medPrice = med;
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

	public Double getHighPercentChange() {
		return highPercentChange;
	}

	public void setHighPercentChange(Double highPercentChange) {
		this.highPercentChange = highPercentChange;
	}

	public Double getLowPercentChange() {
		return lowPercentChange;
	}

	public void setLowPercentChange(Double lowPercentChange) {
		this.lowPercentChange = lowPercentChange;
	}

	public Double getMedPercentChange() {
		return medPercentChange;
	}

	public void setMedPercentChange(Double medPercentChange) {
		this.medPercentChange = medPercentChange;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		time = new Date(in.readLong());
		ticker = in.readUTF();
		name = in.readUTF();
		highPrice = in.readDouble();
		lowPrice = in.readDouble();
		medPrice = in.readDouble();
		numShares = in.readLong();
		numTrades = in.readLong();
		highPercentChange = in.readDouble();
		lowPercentChange = in.readDouble();
		medPercentChange = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(getTimeInMinutesSinceEpoch());
		out.writeUTF(ticker);
		out.writeUTF(name);
		out.writeDouble(highPrice);
		out.writeDouble(lowPrice);
		out.writeDouble(medPrice);
		out.writeLong(numShares);
		out.writeLong(numTrades);
		out.writeDouble(highPercentChange);
		out.writeDouble(lowPercentChange);
		out.writeDouble(medPercentChange);
	}
	
	public long getTimeInMinutesSinceEpoch() {
		long minutesSinceEpoch = time.getTime() / (60 * 1000);
		return minutesSinceEpoch;
	}
}
