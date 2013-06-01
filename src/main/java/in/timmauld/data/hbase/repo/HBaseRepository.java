package in.timmauld.data.hbase.repo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseRepository {

	private Configuration conf = null;
	private HBaseAdmin admin = null;

	public HBaseRepository() throws IOException {
		Configuration newConf = HBaseConfiguration.create();
		this.conf = newConf;
		this.admin = new HBaseAdmin(conf);
	}

	public HBaseRepository(Configuration conf) throws IOException {
		this.conf = conf;
		this.admin = new HBaseAdmin(conf);
	}

	public Configuration getConfiguration() {
		return conf;
	}

	public boolean existsTable(String table) throws IOException {
		return admin.tableExists(table);
	}

	public void createTable(String table, String... colfams) throws IOException {
		createTable(table, null, colfams);
	}

	public void createTable(String table, byte[][] splitKeys, String... colfams)
			throws IOException {
		HTableDescriptor desc = new HTableDescriptor(table);
		for (String cf : colfams) {
			HColumnDescriptor coldef = new HColumnDescriptor(cf);
			desc.addFamily(coldef);
		}
		if (splitKeys != null) {
			admin.createTable(desc, splitKeys);
		} else {
			admin.createTable(desc);
		}
	}

	public void disableTable(String table) throws IOException {
		admin.disableTable(table);
	}

	public void dropTable(String table) throws IOException {
		if (existsTable(table)) {
			disableTable(table);
			admin.deleteTable(table);
		}
	}

	public String padNum(int num, int pad) {
		String res = Integer.toString(num);
		if (pad > 0) {
			while (res.length() < pad) {
				res = "0" + res;
			}
		}
		return res;
	}

	public Result getRowByKey(String table, String rowKey) throws IOException {
		Result r = null;
		HTable tbl = new HTable(conf, table);
		try {
			Get g = new Get(Bytes.toBytes(rowKey));
			r = tbl.get(g);
		} finally {
			tbl.close();
		}
		return r;
	}

	public void put(String table, String row, String fam, String qual, long ts,
			String val) throws IOException {
		HTable tbl = new HTable(conf, table);
		Put put = new Put(Bytes.toBytes(row));
		put.add(Bytes.toBytes(fam), Bytes.toBytes(qual), ts, Bytes.toBytes(val));
		tbl.put(put);
		tbl.close();
	}

	public void put(String table, String[] rows, String[] fams, String[] quals,
			long[] ts, String[] vals) throws IOException {
		HTable tbl = new HTable(conf, table);
		for (String row : rows) {
			Put put = new Put(Bytes.toBytes(row));
			for (String fam : fams) {
				int v = 0;
				for (String qual : quals) {
					String val = vals[v < vals.length ? v : vals.length - 1];
					long t = ts[v < ts.length ? v : ts.length - 1];
					put.add(Bytes.toBytes(fam), Bytes.toBytes(qual), t,
							Bytes.toBytes(val));
					v++;
				}
			}
			tbl.put(put);
		}
		tbl.close();
	}

	public void dump(String table, String[] rows, String[] fams, String[] quals)
			throws IOException {
		HTable tbl = new HTable(conf, table);
		try {
			List<Get> gets = new ArrayList<Get>();
			for (String row : rows) {
				Get get = new Get(Bytes.toBytes(row));
				get.setMaxVersions();
				if (fams != null) {
					for (String fam : fams) {
						for (String qual : quals) {
							get.addColumn(Bytes.toBytes(fam),
									Bytes.toBytes(qual));
						}
					}
				}
				gets.add(get);
			}
			Result[] results = tbl.get(gets);
			for (Result result : results) {
				for (KeyValue kv : result.raw()) {
					System.out.println("KV: " + kv + ", Value: "
							+ Bytes.toString(kv.getValue()));
				}
			}
		} finally {
			tbl.close();
		}
	}
}
