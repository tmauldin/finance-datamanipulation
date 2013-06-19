package in.timmauld.finance.taqimport.data.model.time;

import java.util.Date;

public class DaysBehavior implements DateBehavior{

	@Override
	public String getOutputPath() {
		return "days";
	}

	@Override
	public long getEpochTime(Date time) {
		return time.getTime() / (24 * 60 * 60 * 1000);
	}

}
