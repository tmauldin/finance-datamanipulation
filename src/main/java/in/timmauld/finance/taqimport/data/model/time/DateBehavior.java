package in.timmauld.finance.taqimport.data.model.time;

import java.util.Date;

public interface DateBehavior {
	public String getOutputPath();
	public long getEpochTime(Date time);
}
