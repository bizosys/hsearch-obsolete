package com.bizosys.hsearch.dictionary;

import java.util.ArrayList;
import java.util.List;

import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.hbase.IScanCallBack;

public class DictionaryBook implements IScanCallBack {
	private List<String> lines = new ArrayList<String>();

	private int cursor = 0;
	private int WORDS_IN_A_LINE = 0;
	private char WORD_SEPARATOR = ' ';
	private int keyPrefixLen = 0;
	
	public StringBuilder aLine = new StringBuilder(4096);
	
	public DictionaryBook(int maxWordsInALine, char wordSeparator, String keyPrefix) {
		this.WORDS_IN_A_LINE = maxWordsInALine;
		this.WORD_SEPARATOR = wordSeparator;
		if ( null != keyPrefix) {
			this.keyPrefixLen = keyPrefix.length();
		}
	}

	public void process(byte[] storedBytes) {
		String word = Storable.getString(storedBytes);

		if ( cursor < WORDS_IN_A_LINE) {
			if ( cursor != 0) aLine.append(WORD_SEPARATOR);
			
			if ( this.keyPrefixLen > 0) aLine.append(word.substring(this.keyPrefixLen));
			else aLine.append(word);

			cursor++;
		} else {
			this.lines.add(aLine.toString());
			aLine.delete(0, aLine.capacity());
			cursor = 0;
		}		
	}
	
	/**
	 * Push the last remaining words to the last line
	 * @return
	 */
	public List<String> getLines() {
		String lastLine = aLine.toString();
		aLine.delete(0, aLine.capacity());
		this.lines.add(lastLine);
		return this.lines;
	}
}
