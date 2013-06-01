package in.timmauld.finance.googletrends.data;

public class Company {
	
	private String ticker;
	private String name;
	
	public Company(String ticker, String name) {
		this.ticker = ticker;
		this.name = name;
	}
	
	public String getTicker() {
		return ticker;
	}
	
	public void setTicker(String ticker) {		
		this.ticker = ticker;
	}		
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
}
