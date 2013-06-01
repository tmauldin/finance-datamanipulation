package in.timmauld.finance.taqimport.data;

import in.timmauld.data.hbase.repo.HBaseRepository;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class TaqRepository extends HBaseRepository {

	public enum TaqTable {
		TAQMinute, TAQThirtyMinute, TAQHour, TAQTwoHour, TAQHalfDay, TAQDay, TAQWeek, TAQMonth;

		public static String[] getNames() {
			String[] names = new String[TaqTable.values().length];
			for (int i = 0; i < TaqTable.values().length; i++) {
				names[i] = TaqTable.values()[i].name();
			}
			return names;
		}
	}

	public enum ColumnFamily {
		price, volume, derivedattributes;

		public static String[] getNames() {
			String[] names = new String[ColumnFamily.values().length];
			for (int i = 0; i < ColumnFamily.values().length; i++) {
				names[i] = ColumnFamily.values()[i].name();
			}
			return names;
		}
	}

	public enum PriceColumn {
		high, low, med;

		public static String[] getNames() {
			String[] names = new String[PriceColumn.values().length];
			for (int i = 0; i < PriceColumn.values().length; i++) {
				names[i] = PriceColumn.values()[i].name();
			}
			return names;
		}
	}

	public static enum VolumeColumn {
		numtrades, numsharestraded;

		public static String[] getNames() {
			String[] names = new String[VolumeColumn.values().length];
			for (int i = 0; i < VolumeColumn.values().length; i++) {
				names[i] = VolumeColumn.values()[i].name();
			}
			return names;
		}
	}

	public TaqRepository() throws IOException {
		super();
	}

	public void createTaqTableIfNotExists(TaqTable hbTable) throws IOException {
		if (!existsTable(hbTable.name())) {
			createTable(hbTable.name(), ColumnFamily.getNames());
		}
	}

	public void addTaqRow(TaqAggregationWritable taqDto, TaqTable hbTable) throws IOException {
		HTable tbl = new HTable(getConfiguration(), hbTable.name());
		try {

			// Check for existence, return if it does
			String rowKey = taqDto.getTicker() + "_" + taqDto.getTimeInMinutesSinceEpoch();
			if (getRowByKey(hbTable.name(), rowKey) != null) {
				return;
			}

			// Create the row
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(ColumnFamily.price.name()),
					Bytes.toBytes(PriceColumn.high.name()),
					Bytes.toBytes(taqDto.getHigh().toString()));
			put.add(Bytes.toBytes(ColumnFamily.price.name()),
					Bytes.toBytes(PriceColumn.low.name()),
					Bytes.toBytes(taqDto.getLow().toString()));
			put.add(Bytes.toBytes(ColumnFamily.price.name()),
					Bytes.toBytes(PriceColumn.med.name()),
					Bytes.toBytes(taqDto.getMed().toString()));
			put.add(Bytes.toBytes(ColumnFamily.volume.name()),
					Bytes.toBytes(VolumeColumn.numtrades.name()),
					Bytes.toBytes(taqDto.getNumTrades() + ""));
			put.add(Bytes.toBytes(ColumnFamily.volume.name()),
					Bytes.toBytes(VolumeColumn.numsharestraded.name()),
					Bytes.toBytes(taqDto.getNumShares() + ""));
			tbl.put(put);
		} finally {
			tbl.close();
		}
	}
}
