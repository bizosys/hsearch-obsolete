package com.bizosys.hsearch.filter;

public class AMMarker {
	public int serial = 0;
	public int aclStart = 0; 
	public int aclEnd = 0;
	public int metaStart = 0; 
	public int metaEnd = 0;
	
	public AMMarker () {
	}
	
	public AMMarker(int serial, int aclStart, int aclEnd, int metaStart, int metaEnd ) {
		set(serial, aclStart,aclEnd,metaStart,metaEnd);
	}
	
	public void set (int serial, int aclStart, int aclEnd,
			int metaStart, int metaEnd ) {
		this.serial = serial;
		this.aclStart = aclStart;
		this.aclEnd = aclEnd;
		this.metaStart = metaStart;
		this.metaEnd = metaEnd;
	}
}
