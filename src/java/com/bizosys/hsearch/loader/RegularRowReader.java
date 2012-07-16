package com.bizosys.hsearch.loader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

public class RegularRowReader implements RowReader {

	private static final boolean DEBUG_ENABLED = LoaderLog.l.isDebugEnabled();
	BufferedReader br = null;
	char separator = ',';
	int expectedRow = -1;
	
	public RegularRowReader(Reader reader, String separator) {
		this.br = new BufferedReader(reader);
		this.separator = separator.charAt(0);
	}
	
	public String[] readNext() throws IOException {
		String nextLine = br.readLine();
		if ( DEBUG_ENABLED)
			LoaderLog.l.debug("RegularRowReader:readNext() > " + nextLine);
	    if (nextLine == null) return null;
	    if ( 0 == nextLine.length()) return null;
	    
	    String[] cells = fastSplit(nextLine);
	    return cells;
	}

	public void close() throws IOException {
		if ( null != this.br)
			this.br.close();
	}
	

	public String[] fastSplit(String text) {
		if ( -1 == expectedRow) {
			expectedRow = 0;
			  int index1 = 0;
			  int index2 = text.indexOf(separator);
			  while (index2 >= 0) {
				  index1 = index2 + 1;
				  index2 = text.indexOf(separator, index1);
				  expectedRow++;
			  }
		            
			  if (index1 < text.length() - 1) {
				  expectedRow++;
			  }
		}
		
		String[] cells = new String[expectedRow];
		
		int cellPos = 0;
		int index1 = 0;
		int index2 = text.indexOf(separator);
		String token = null;
		while (index2 >= 0) {
			token = text.substring(index1, index2);
			cells[cellPos] = token;
			index1 = index2 + 1;
			index2 = text.indexOf(separator, index1);
			cellPos++;
		}
	            
		if (index1 < text.length() - 1) {
			cells[cellPos] = text.substring(index1);
		}
		return cells;			
	}
}
