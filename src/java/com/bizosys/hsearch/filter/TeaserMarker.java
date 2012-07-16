package com.bizosys.hsearch.filter;

public class TeaserMarker {
	public int serial = 0;

	public int start = 0; 
	public int end = 0;
	
	public int iutp = 0;
	public int cacheStart = 0;
	public int cacheEnd = 0;
	public int cacheLen = 0;
	
	public TeaserMarker(int serial, byte[] data, int bytePos, 
		TeaserFilterCommon tf, int sectionSize) {
		
		if ( null == data) return;
		
		this.serial = serial;
		this.start = bytePos;
		
		int idLen = Storable.getShort(bytePos, data);
		bytePos = bytePos + 2;
		
		int urlLen = Storable.getShort(bytePos, data);
		bytePos = bytePos + 2;
		
		int titleLen = Storable.getShort(bytePos, data);
		bytePos = bytePos + 2;
		
		int previewLen = Storable.getInt(bytePos, data);
		bytePos = bytePos + 4;
		
		int cacheSize = Storable.getInt(bytePos, data);
		bytePos = bytePos + 4;
		
		this.iutp = idLen + urlLen + titleLen + previewLen;
		this.cacheStart = bytePos + this.iutp;
		this.end = this.cacheStart + cacheSize;

		if ( cacheSize == 0) {
			this.cacheEnd = this.cacheStart;
			return;
		}
		
		//Modify the cache
		tf.setContent(data, this.cacheStart, cacheSize);
		
		if ( data.length < sectionSize) sectionSize = data.length;
		if ( cacheSize < sectionSize) sectionSize = cacheSize;
		
		int[] markings  = tf.mark(sectionSize);
		if ( null != markings) {
			this.cacheStart = markings[0];
			this.cacheEnd = markings[1];
		} else {
			this.cacheEnd = this.cacheStart + sectionSize;
		}
		
		this.cacheLen = this.cacheEnd - this.cacheStart;
		
		if ( this.cacheLen < 0 ) {
			System.err.println("Invalid Teaser markings :" + this.cacheStart + "/" + this.cacheEnd );
			this.cacheEnd = this.cacheStart + sectionSize;
		} else if ( this.cacheLen > sectionSize) {
			this.cacheEnd = this.cacheStart + sectionSize;
		}
	}
	
	public int getNewSize() {
		return 14 + this.iutp + this.cacheLen;
	}
	
	public int extract(byte[] source, byte[] dest, int destPos) {
		System.arraycopy(source, this.start, dest, destPos, 10);
		destPos = destPos + 10;
		
		byte[] cacheLenB = Storable.putInt(this.cacheLen);
		System.arraycopy(cacheLenB, 0, dest, destPos, 4);
		destPos = destPos + 4;
		
		System.arraycopy(source, this.start + 14, dest, destPos, this.iutp);
		destPos = destPos + this.iutp;
		System.arraycopy(source, this.cacheStart, dest, destPos, this.cacheLen);
		destPos = destPos + this.cacheLen;
		return destPos;
	}
	
	public static int measure(byte[] data, int bytePos) {
			
			int idLen = Storable.getShort(bytePos, data);
			bytePos = bytePos + 2;
			
			int urlLen = Storable.getShort(bytePos, data);
			bytePos = bytePos + 2;
			
			int titleLen = Storable.getShort(bytePos, data);
			bytePos = bytePos + 2;
			
			int previewLen = Storable.getInt(bytePos, data);
			bytePos = bytePos + 4;
			
			int cacheLen = Storable.getInt(bytePos, data);
			bytePos = bytePos + 4;
			
			return 14 + idLen + urlLen + titleLen + previewLen + cacheLen;
			
		}	
}
