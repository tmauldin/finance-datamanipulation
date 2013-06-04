package in.timmauld.finance.googletrends.data;

import in.timmauld.hbase.data.HBaseRepository;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class CompanyRepository extends HBaseRepository {
	
	final String TABLE_TAQ_MINUTE = "TAQ_Minute";
	final String FAMILY_COMPANY = "company";
	final String QUAL_TICKER = "ticker";
	final String QUAL_COMPANY_NAME = "name";
	
	public CompanyRepository() throws IOException {
		super("TAQ_Minute", null);			
	}
	
	public ArrayList<Company> getAllCompanies() throws IOException {
		ArrayList<Company> companies = new ArrayList<Company>();
		HTable table = new HTable(getConfiguration(), TABLE_TAQ_MINUTE);
		Scan s = new Scan();
		s.addFamily(Bytes.toBytes(FAMILY_COMPANY));
		ResultScanner scanner = table.getScanner(s);
		try {
			for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
				byte[] companyName = rr.getValue(Bytes.toBytes(FAMILY_COMPANY), Bytes.toBytes(QUAL_COMPANY_NAME));
				byte[] ticker = rr.getValue(Bytes.toBytes(FAMILY_COMPANY), Bytes.toBytes(QUAL_TICKER));
				companies.add(new Company(Bytes.toString(ticker), Bytes.toString(companyName)));
			}
		} finally {
			scanner.close();
			table.close();
		}
		return companies;		
	}
}
