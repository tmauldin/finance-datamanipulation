package in.timmauld.finance.taqimport.data.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TaqReturnsWritable extends TaqAggregationWritable {
	
	private Double highPercentChange;
	private Double lowPercentChange;
	private Double meanPercentChange;
		
	public TaqReturnsWritable() {
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

	public Double getMeanPercentChange() {
		return meanPercentChange;
	}

	public void setMeanPercentChange(Double meanPercentChange) {
		this.meanPercentChange = meanPercentChange;
	}

	public void readFields(DataInput in) throws IOException {
		setKey(in.readUTF());
		setTime(in.readLong());
		setTicker(in.readUTF());
		setName(in.readUTF());
		setHighPrice(in.readDouble());
		setLowPrice(in.readDouble());
		setMeanPrice(in.readDouble());
		setNumShares(in.readLong());
		setNumTrades(in.readLong());
		setVariance(in.readDouble());
		highPercentChange = in.readDouble();
		lowPercentChange = in.readDouble();
		meanPercentChange = in.readDouble();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(getKey());
		out.writeLong(getTime());
		out.writeUTF(getTicker());
		out.writeUTF(getName());
		out.writeDouble(getHighPrice());
		out.writeDouble(getLowPrice());
		out.writeDouble(getMeanPrice());
		out.writeLong(getNumShares());
		out.writeLong(getNumTrades());
		out.writeDouble(getVariance());
		out.writeDouble(highPercentChange);
		out.writeDouble(lowPercentChange);
		out.writeDouble(meanPercentChange);
	}
}