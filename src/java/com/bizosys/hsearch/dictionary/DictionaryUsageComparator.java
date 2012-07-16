package com.bizosys.hsearch.dictionary;

import java.util.Comparator;

public class DictionaryUsageComparator implements Comparator<Dictionary> {

	public int compare(Dictionary o1, Dictionary o2) {
		if( o1.touchTime > o2.touchTime) return 1;
		else if(o1.touchTime < o2.touchTime) return -1;
		else return 0;    
	}		

}
