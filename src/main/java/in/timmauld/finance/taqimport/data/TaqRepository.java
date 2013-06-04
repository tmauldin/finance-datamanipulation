package in.timmauld.finance.taqimport.data;

import in.timmauld.hbase.data.Repository;

import java.io.IOException;

public interface TaqRepository extends Repository{

	public void addTaqRow(TaqAggregationWritable taqDto)
			throws IOException;
	
}