package in.timmauld.hbase.data;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;

public interface Repository {

	public Configuration getConfiguration();

	public boolean existsTable(String table) throws IOException;

	public void createTableIfNotExists() throws IOException;
	
	public void disableTable(String table) throws IOException;

	public void dropTable(String table) throws IOException;

	public String padNum(int num, int pad);

	public Result getRowByKey(String table, String rowKey)
			throws IOException;

	public void put(String table, String row, String fam, String qual,
			long ts, String val) throws IOException;

	public void put(String table, String[] rows, String[] fams,
			String[] quals, long[] ts, String[] vals) throws IOException;

	public void dump(String table, String[] rows, String[] fams,
			String[] quals) throws IOException;

}