package in.timmauld.finance.taqimport.data.model.time;

import java.util.Date;

public class ThirtyMinutesBehavior implements DateBehavior{

	@Override
	public String getOutputPath() {
		return "thirtyminutes";
	}

	@Override
	public long getEpochTime(Date time) {
		return time.getTime() / (30 * 60 * 1000);
	}

}
