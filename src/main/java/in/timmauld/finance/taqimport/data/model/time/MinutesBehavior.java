package in.timmauld.finance.taqimport.data.model.time;

import java.util.Date;

public class MinutesBehavior implements DateBehavior{

	@Override
	public String getOutputPath() {
		return "minutes";
	}

	@Override
	public long getEpochTime(Date time) {
		return time.getTime() / (60 * 1000);
	}

}
