package in.timmauld.finance.taqimport.data;

import in.timmauld.finance.taqimport.data.model.TaqAggregationWritable;
import in.timmauld.finance.taqimport.data.model.TaqReturnsWritable;
import in.timmauld.hbase.data.HBaseRepository;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.inject.Inject;

public class TaqHbaseRepository extends HBaseRepository implements TaqRepository {
	
	public interface TaqTable {
		String name();
	}
	
	public enum TaqHBaseTable implements TaqTable{
		TAQMinute, TAQThirtyMinute, TAQHour, TAQTwoHour, TAQHalfDay, TAQDay, TAQWeek, TAQMonth;

		public static String[] getNames() {
			String[] names = new String[values().length];
			for (int i = 0; i < values().length; i++) {
				names[i] = values()[i].name();
			}
			return names;
		}
	}

	public enum ColumnFamily {
		company, price, volume, derived;

		public static String[] getNames() {
			String[] names = new String[values().length];
			for (int i = 0; i < values().length; i++) {
				names[i] = values()[i].name();
			}
			return names;
		}
	}

	public enum CompanyColumn {
		symbol, name;
	}
	
	public enum PriceColumn {
		time, highPrice, lowPrice, meanPrice, highPercentChange, lowPercentChange, meanPercentChange, variance;
	}

	public enum VolumeColumn {
		numtrades, numsharestraded;
	}

	@Inject
	public TaqHbaseRepository(TaqTable tableVal) throws IOException {
		super(tableVal.name(), ColumnFamily.getNames());
	}
	
	/* (non-Javadoc)
	 * @see in.timmauld.finance.taqimport.data.TaqRepository#addTaqRow(in.timmauld.finance.taqimport.data.TaqAggregationWritable, java.lang.String)
	 */
	public Put getTaqAggregationPut(TaqAggregationWritable taqDto) throws IOException {
		HTable tbl = new HTable(getConfiguration(), getTableName());
		Put put = new Put(Bytes.toBytes(taqDto.getKey()));
		try {
			// Create the row			
			put.add(Bytes.toBytes(ColumnFamily.company.name()),
					Bytes.toBytes(CompanyColumn.symbol.name()),
					Bytes.toBytes(taqDto.getTicker().toString()));
			put.add(Bytes.toBytes(ColumnFamily.company.name()),
					Bytes.toBytes(CompanyColumn.name.name()),
					Bytes.toBytes(taqDto.getName().toString()));
			put.add(Bytes.toBytes(ColumnFamily.price.name()),
					Bytes.toBytes(PriceColumn.highPrice.name()),
					Bytes.toBytes(taqDto.getHighPrice().toString()));
			put.add(Bytes.toBytes(ColumnFamily.price.name()),
					Bytes.toBytes(PriceColumn.lowPrice.name()),
					Bytes.toBytes(taqDto.getLowPrice().toString()));
			put.add(Bytes.toBytes(ColumnFamily.price.name()),
					Bytes.toBytes(PriceColumn.meanPrice.name()),
					Bytes.toBytes(taqDto.getMeanPrice().toString()));
			put.add(Bytes.toBytes(ColumnFamily.volume.name()),
					Bytes.toBytes(VolumeColumn.numtrades.name()),
					Bytes.toBytes(taqDto.getNumTrades() + ""));
			put.add(Bytes.toBytes(ColumnFamily.volume.name()),
					Bytes.toBytes(VolumeColumn.numsharestraded.name()),
					Bytes.toBytes(taqDto.getNumShares() + ""));
			put.add(Bytes.toBytes(ColumnFamily.price.name()),
					Bytes.toBytes(PriceColumn.variance.name()),
					Bytes.toBytes(taqDto.getVariance() + ""));

		} finally {
			tbl.close();
		}
		return put;
	}
	
	public TaqReturnsWritable parseHBaseResult(ImmutableBytesWritable row, Result value) throws IOException {		
		TaqReturnsWritable dto = new TaqReturnsWritable();
		dto.setKey(row.toString());
		dto.setTime(Long.parseLong(new String(value.getValue(
				Bytes.toBytes(ColumnFamily.price.name()), 
				Bytes.toBytes(PriceColumn.time.name())))));
		dto.setTicker(new String(value.getValue(
				Bytes.toBytes(ColumnFamily.company.name()), 
				Bytes.toBytes(CompanyColumn.symbol.name()))));		
		dto.setName(new String(value.getValue(
				Bytes.toBytes(ColumnFamily.price.name()), 
				Bytes.toBytes(PriceColumn.highPrice.name()))));
		dto.setHighPrice(Double.parseDouble(new String(value.getValue(
				Bytes.toBytes(ColumnFamily.price.name()), 
				Bytes.toBytes(PriceColumn.highPrice.name())))));		
		dto.setLowPrice(Double.parseDouble(new String(value.getValue(
				Bytes.toBytes(ColumnFamily.price.name()), 
				Bytes.toBytes(PriceColumn.lowPrice.name())))));	
		dto.setMeanPrice(Double.parseDouble(new String(value.getValue(
				Bytes.toBytes(ColumnFamily.price.name()), 
				Bytes.toBytes(PriceColumn.meanPrice.name())))));	
		dto.setNumShares(Long.parseLong(new String(value.getValue(
				Bytes.toBytes(ColumnFamily.volume.name()), 
				Bytes.toBytes(VolumeColumn.numsharestraded.name())))));
		dto.setNumTrades(Long.parseLong(new String(value.getValue(
				Bytes.toBytes(ColumnFamily.volume.name()), 
				Bytes.toBytes(VolumeColumn.numtrades.name())))));
		dto.setHighPercentChange(Double.parseDouble(new String(value.getValue(
				Bytes.toBytes(ColumnFamily.price.name()), 
				Bytes.toBytes(PriceColumn.highPercentChange.name())))));
		dto.setLowPercentChange(Double.parseDouble(new String(value.getValue(
				Bytes.toBytes(ColumnFamily.price.name()), 
				Bytes.toBytes(PriceColumn.lowPercentChange.name())))));
		dto.setMeanPercentChange(Double.parseDouble(new String(value.getValue(
				Bytes.toBytes(ColumnFamily.price.name()), 
				Bytes.toBytes(PriceColumn.meanPercentChange.name())))));
		return dto;
	}
	
	public Put getTaqReturnsPut(TaqReturnsWritable taqDto) throws IOException {
		HTable tbl = new HTable(getConfiguration(), getTableName());
		Put put = new Put(Bytes.toBytes(taqDto.getKey()));
		try {	
			// Create the row			
			put.add(Bytes.toBytes(ColumnFamily.company.name()),
					Bytes.toBytes(CompanyColumn.symbol.name()),
					Bytes.toBytes(taqDto.getTicker().toString()));
			put.add(Bytes.toBytes(ColumnFamily.company.name()),
					Bytes.toBytes(CompanyColumn.name.name()),
					Bytes.toBytes(taqDto.getName().toString()));
			put.add(Bytes.toBytes(ColumnFamily.price.name()),
					Bytes.toBytes(PriceColumn.time.name()),
					Bytes.toBytes(taqDto.getTime() + ""));
			put.add(Bytes.toBytes(ColumnFamily.price.name()),
					Bytes.toBytes(PriceColumn.highPrice.name()),
					Bytes.toBytes(taqDto.getHighPrice().toString()));
			put.add(Bytes.toBytes(ColumnFamily.price.name()),
					Bytes.toBytes(PriceColumn.lowPrice.name()),
					Bytes.toBytes(taqDto.getLowPrice().toString()));
			put.add(Bytes.toBytes(ColumnFamily.price.name()),
					Bytes.toBytes(PriceColumn.meanPrice.name()),
					Bytes.toBytes(taqDto.getMeanPrice().toString()));
			put.add(Bytes.toBytes(ColumnFamily.volume.name()),
					Bytes.toBytes(VolumeColumn.numtrades.name()),
					Bytes.toBytes(taqDto.getNumTrades() + ""));
			put.add(Bytes.toBytes(ColumnFamily.volume.name()),
					Bytes.toBytes(VolumeColumn.numsharestraded.name()),
					Bytes.toBytes(taqDto.getNumShares() + ""));
			put.add(Bytes.toBytes(ColumnFamily.price.name()),
					Bytes.toBytes(PriceColumn.variance.name()),
					Bytes.toBytes(taqDto.getVariance() + ""));
			put.add(Bytes.toBytes(ColumnFamily.price.name()),
					Bytes.toBytes(PriceColumn.highPercentChange.name()),
					Bytes.toBytes(taqDto.getHighPercentChange().toString()));
			put.add(Bytes.toBytes(ColumnFamily.price.name()),
					Bytes.toBytes(PriceColumn.lowPercentChange.name()),
					Bytes.toBytes(taqDto.getLowPercentChange().toString()));
			put.add(Bytes.toBytes(ColumnFamily.price.name()),
					Bytes.toBytes(PriceColumn.meanPercentChange.name()),
					Bytes.toBytes(taqDto.getMeanPercentChange().toString()));
		} finally {
			tbl.close();
		}
		return put;
	}	
	
}
