package in.timmauld.finance.taqimport.data.model.time;

import java.util.Date;

public class SecondsBehavior implements DateBehavior{

	@Override
	public String getOutputPath() {
		return "seconds";
	}

	@Override
	public long getEpochTime(Date time) {
		return time.getTime() / 1000;
	}

}
