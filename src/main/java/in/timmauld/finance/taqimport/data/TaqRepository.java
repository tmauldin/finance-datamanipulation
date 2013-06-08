package in.timmauld.finance.taqimport.data;

import in.timmauld.finance.taqimport.data.model.TaqAggregationWritable;
import in.timmauld.finance.taqimport.data.model.TaqReturnsWritable;
import in.timmauld.hbase.data.Repository;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public interface TaqRepository extends Repository{

	public static final String TAQ_TABLE_CONFIG_NAME = "taqtable";
	
	public Put getTaqAggregationPut(TaqAggregationWritable taqDto) throws IOException;
	
	public TaqReturnsWritable parseHBaseResult(ImmutableBytesWritable row, Result value) throws IOException;
	
	public Put getTaqReturnsPut(TaqReturnsWritable taqDto) throws IOException;
	
}