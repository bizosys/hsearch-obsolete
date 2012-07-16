package com.bizosys.hsearch.loader;

import java.io.Reader;

import com.bizosys.hsearch.loader.csv.CsvReader;

public class RowReaderFactory {
	
	public static RowReader getReader(String separator, Reader reader) {
		if ( ",".equals(separator)) return new CsvReader(reader);
		else return new RegularRowReader(reader, separator);
	}
}
