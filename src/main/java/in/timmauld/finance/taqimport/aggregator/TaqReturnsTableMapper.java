package in.timmauld.finance.taqimport.aggregator;

import in.timmauld.finance.taqimport.data.TaqHbaseRepository;
import in.timmauld.finance.taqimport.data.TaqRepository;
import in.timmauld.finance.taqimport.data.TaqHbaseRepository.TaqHBaseTable;
import in.timmauld.finance.taqimport.data.model.TaqReturnsWritable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;

public class TaqReturnsTableMapper extends TableMapper<Text, TaqReturnsWritable>  {

	private TaqRepository repository;
	
   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
   		TaqReturnsWritable taq = repository.parseHBaseResult(row, value);
        context.write(new Text(taq.getTicker()), taq);
   	}
   	
   	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		// TODO put DI back in
		TaqRepository repository = new TaqHbaseRepository(TaqHBaseTable.TAQMinute);
		Configuration conf = context.getConfiguration();
		String tableName = conf.get(TaqRepository.TAQ_TABLE_CONFIG_NAME);
		repository.setTableName(tableName);
		repository.createTableIfNotExists();
	}
}
