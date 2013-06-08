package in.timmauld.finance.taqimport.main;

import in.timmauld.finance.taqimport.data.TaqDIModule;
import in.timmauld.finance.taqimport.data.TaqRepository;
import in.timmauld.finance.taqimport.data.model.TaqReturnsWritable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class TaqReturnsTableMapper extends TableMapper<Text, TaqReturnsWritable>  {

	private TaqRepository repository;
	
   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
   		TaqReturnsWritable taq = repository.parseHBaseResult(row, value);
        context.write(new Text(taq.getTicker()), taq);
   	}
   	
   	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		// Inject repository dependency
		Injector injector = Guice.createInjector(new TaqDIModule());
		repository = injector.getInstance(TaqRepository.class);
		Configuration conf = context.getConfiguration();
		String tableName = conf.get(TaqRepository.TAQ_TABLE_CONFIG_NAME);
		repository.setTableName(tableName);
		repository.createTableIfNotExists();
	}
}
