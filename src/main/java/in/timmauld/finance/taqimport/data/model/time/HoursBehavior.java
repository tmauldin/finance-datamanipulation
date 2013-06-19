package in.timmauld.finance.taqimport.data.model.time;

import java.util.Date;

public class HoursBehavior implements DateBehavior{

	@Override
	public String getOutputPath() {
		return "hours";
	}

	@Override
	public long getEpochTime(Date time) {
		return time.getTime() / (60 * 60 * 1000);
	}

}
