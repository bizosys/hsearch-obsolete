package com.bizosys.hsearch.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import com.bizosys.hsearch.filter.TeaserFilterCommon.WordPosition;

public class FilterObjectFactory {
	private static FilterObjectFactory thisInstance = new FilterObjectFactory();
	public static FilterObjectFactory getInstance() {
		return thisInstance;
	}
	
	private static int MINIMUM_CACHE = 10;
	private static int MAXIMUM_CACHE = 4096;

	Stack<AccessStorable> accesses = new Stack<AccessStorable>();
	Stack<List<TeaserMarker>> teaserMarkers =  new Stack<List<TeaserMarker>>();
	Stack<List<AMMarker>> amMarkers = new Stack<List<AMMarker>>();
	Stack<List<WordPosition>> wordPositions =  new Stack<List<WordPosition>>();
	Stack<AMMarker> oneMarker = new Stack<AMMarker>();
	
	public  AccessStorable getStorableAccess() {
		AccessStorable entry = null;
		if (accesses.size() > MINIMUM_CACHE ) entry = accesses.pop();
		if ( null != entry ) return entry;
		return new AccessStorable();
	}
	
	public  void putStorableAccess(AccessStorable entry ) {
		if ( null == entry) return;
		entry.clear();
		if (accesses.size() > MAXIMUM_CACHE ) return;
		if (accesses.contains(entry) ) return;
		accesses.add(entry);
	}
	
	public  List<TeaserMarker> getTeaserMarker() {
		List<TeaserMarker> entry = null;
		if (teaserMarkers.size() > MINIMUM_CACHE ) entry = teaserMarkers.pop();
		if ( null != entry ) return entry;
		return new ArrayList<TeaserMarker>();
	}
	
	public  void putTeaserMarker(List<TeaserMarker> entry ) {
		if ( null == entry) return;
		entry.clear();
		if (teaserMarkers.size() > MAXIMUM_CACHE ) return;
		if (teaserMarkers.contains(entry) ) return;
		teaserMarkers.add(entry);
	}	
	
	public  List<AMMarker> getAMMarkers() {
		List<AMMarker> entry = null;
		if (amMarkers.size() > MINIMUM_CACHE ) entry = amMarkers.pop();
		if ( null != entry ) return entry;
		return new ArrayList<AMMarker>();
	}
	
	public  void putAMMarkers(List<AMMarker> entry ) {
		if ( null == entry) return;
		entry.clear();
		if (teaserMarkers.size() > MAXIMUM_CACHE ) return;
		if (teaserMarkers.contains(entry) ) return;
		amMarkers.add(entry);
	}		
	
	public  List<WordPosition> getWordPosition() {
		List<WordPosition> entry = null;
		if (wordPositions.size() > MINIMUM_CACHE ) entry = wordPositions.pop();
		if ( null != entry ) return entry;
		return new ArrayList<WordPosition>();
	}
	
	public  void putWordPosition(List<WordPosition> entry ) {
		if ( null == entry) return;
		entry.clear();
		if (wordPositions.size() > MAXIMUM_CACHE ) return;
		if (wordPositions.contains(entry) ) return;
		wordPositions.add(entry);
	}		
	
	public AMMarker getOneAMMarker() {
		AMMarker entry = null;
		if (oneMarker.size() > MINIMUM_CACHE ) entry = oneMarker.pop();
		if ( null != entry ) return entry;
		return new AMMarker();
	}
	
	public  void putOneAMMarker(AMMarker entry ) {
		if ( null == entry) return;
		if (oneMarker.size() > MAXIMUM_CACHE ) return;
		if (oneMarker.contains(entry) ) return;
		oneMarker.add(entry);
	}	
}
