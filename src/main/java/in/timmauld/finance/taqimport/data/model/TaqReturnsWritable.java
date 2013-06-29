package in.timmauld.finance.taqimport.data.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public class TaqReturnsWritable extends TaqAggregationWritable {
	
	private BigDecimal highPercentChange;
	private BigDecimal lowPercentChange;
	private BigDecimal meanPercentChange;
		
	public TaqReturnsWritable() {
	}
	
	public BigDecimal getHighPercentChange() {
		return highPercentChange;
	}

	public void setHighPercentChange(BigDecimal highPercentChange) {
		this.highPercentChange = highPercentChange;
	}

	public BigDecimal getLowPercentChange() {
		return lowPercentChange;
	}

	public void setLowPercentChange(BigDecimal lowPercentChange) {
		this.lowPercentChange = lowPercentChange;
	}

	public BigDecimal getMeanPercentChange() {
		return meanPercentChange;
	}

	public void setMeanPercentChange(BigDecimal meanPercentChange) {
		this.meanPercentChange = meanPercentChange;
	}

	public void readFields(DataInput in) throws IOException {
		setKey(in.readUTF());
		setTime(in.readLong());
		setTicker(in.readUTF());
		setName(in.readUTF());
		setHighPrice(new BigDecimal(in.readUTF()));
		setLowPrice(new BigDecimal(in.readUTF()));
		setMeanPrice(new BigDecimal(in.readUTF()));
		setNumShares(in.readLong());
		setNumTrades(in.readLong());
		setVariance(in.readDouble());
		highPercentChange = new BigDecimal(in.readUTF());
		lowPercentChange = new BigDecimal(in.readUTF());
		meanPercentChange = new BigDecimal(in.readUTF());
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(getKey());
		out.writeLong(getTime());
		out.writeUTF(getTicker());
		out.writeUTF(getName());
		out.writeUTF(getHighPrice().toString());
		out.writeUTF(getLowPrice().toString());
		out.writeUTF(getMeanPrice().toString());
		out.writeLong(getNumShares());
		out.writeLong(getNumTrades());
		out.writeDouble(getVariance());
		out.writeUTF(highPercentChange.toString());
		out.writeUTF(lowPercentChange.toString());
		out.writeUTF(meanPercentChange.toString());
	}
	
	@Override
	public boolean equals(Object obj) {
		super.equals(obj);
		TaqReturnsWritable taqToCompare = (TaqReturnsWritable)obj;
		if (!this.getHighPercentChange().equals(taqToCompare.getHighPercentChange())) {
			return false;
		}
		if (!this.getLowPercentChange().equals(taqToCompare.getLowPercentChange())) {
			return false;
		}
		if (!this.getMeanPercentChange().equals(taqToCompare.getMeanPercentChange())) {
			return false;
		}
		return true;
	}
	
	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}
}