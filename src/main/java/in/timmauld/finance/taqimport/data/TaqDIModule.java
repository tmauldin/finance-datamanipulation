package in.timmauld.finance.taqimport.data;

import in.timmauld.finance.taqimport.data.TaqHbaseRepository.TaqHBaseTable;
import in.timmauld.finance.taqimport.data.TaqHbaseRepository.TaqTable;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class TaqDIModule extends AbstractModule {
	
	  @Override 
	  protected void configure() {
	    bind(TaqRepository.class).to(TaqHbaseRepository.class);
	  }
	  
	  @Provides
	  TaqTable provideTaqTable() {
	    return TaqHBaseTable.TAQMinute;
	  }
}
