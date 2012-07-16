package com.bizosys.hsearch.common;

public class AutoIncrIdRange {
	public short startPosition = -1;
	public short totalAmount = -1;
	
	public AutoIncrIdRange(short startPosition, short totalAmount ) {
		this.startPosition = startPosition;
		this.totalAmount = totalAmount;
	}
	
	@Override
	public String toString() {
		return this.startPosition + "/" + this.totalAmount;
	}
}
