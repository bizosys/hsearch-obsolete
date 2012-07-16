package com.bizosys.hsearch.dictionary;

import java.io.IOException;
import java.io.Writer;

import org.apache.log4j.Logger;

import com.bizosys.hsearch.hbase.IScanCallBack;

public class StreamDictionaryEntries implements IScanCallBack {

	public static Logger l = Logger.getLogger(StreamDictionaryEntries.class.getName());
	
	Writer writer = null;
	boolean isFault = false;
	
	public StreamDictionaryEntries(Writer writer) {
		this.writer = writer;
	}
	
	public void process(byte[] storedBytes) {
		if ( null == storedBytes) return;
		if ( isFault ) return;
		
		try {
			DictEntry dictEntry = new DictEntry(storedBytes, 0);
			dictEntry.toXml(writer);
		} catch (IOException ex) {
			l.fatal(ex);
			isFault = true;
		}
	}
}
