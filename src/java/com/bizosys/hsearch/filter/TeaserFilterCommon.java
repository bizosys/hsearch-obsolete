/*
* Copyright 2010 Bizosys Technologies Limited
*
* Licensed to the Bizosys Technologies Limited (Bizosys) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The Bizosys licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.bizosys.hsearch.filter;

import java.util.List;

/**
 * Exract relevant section of document for showing in the search result.
 * @author karan
 *
 */
public class TeaserFilterCommon {
	
	/**
	 * The special characters for cutting the document
	 * We have removed [;] character as this conflicts with XML escape Ex. &gt;
	 */
	private static final byte[] WORD_DELIMITERS = new String(" .,\r\n-").getBytes();
	private static final int WORD_DELIMITERS_LENGTH = WORD_DELIMITERS.length;
	
	private static final byte[] LINE_DELIMITERS = new String(".\r\n-").getBytes();
	private static final int LINE_DELIMITERS_LENGTH = LINE_DELIMITERS.length;

	/**
	 * Byte content 
	 */
	private byte[] bContent;
	
	/**
	 * Location from which to start reading
	 */
	private int offset = 0;
	
	/**
	 * Till which location we need to read
	 */
	private int endPos = 0;
	
	/**
	 * Found matching words in bytes
	 */
	private byte[][] bWords;
	
	/**
	 * Input contnet size
	 */
	private int csize;
	
	/**
	 * Matching words sizes
	 */
	private int wsize[];
	
	/**
	 * Default Constructor
	 *
	 */
	public TeaserFilterCommon() {
	}
	
	public TeaserFilterCommon(byte[][] words) {
		setWords(words);
	}

	public TeaserFilterCommon(byte[] content, byte[][] words) {
		setContent(content, 0, -1);
		setWords(words);
	}

	/**
	 * Constructor
	 * @param content	Content bytes
	 * @param words	Matching words sections
	 */
	public TeaserFilterCommon(byte[] content, int offset, int length, byte[][] words) {
		setContent(content, offset, length);
		setWords(words);
	}
	
	/**
	 * Set the matching words
	 * @param words	The matching words
	 */
	public void setWords(byte[][] words) {
		this.bWords = words;

		int wordsT = words.length;
		this.wsize = new int[wordsT];
		for (int i=0; i<wordsT; i++) {
			this.wsize[i] = words[i].length;
		}
	}	
	
	public void setContent(byte[] content, int offset, int length) {
		this.bContent = content;
		if ( null == content ) return;
		this.offset = offset;
		this.csize = length;
		if ( -1 == length) this.csize  = content.length - offset - 1;
		else {
			if ( this.csize > (offset + content.length) ) this.csize =  content.length - offset - 1;
			if ( this.csize < 0 ) this.csize = 0;
		}
		this.endPos = this.offset + this.csize - 1;
	}
	
	/**
	 * Extract the most suitable section of matching words 
	 * @param sectionSize	The teaser section size (e.g. 300 words)
	 * @return	byte[] The content section on bytes
	 */
	public byte[] find(int sectionSize) {
		if ( null == this.bContent) return null;
		List<WordPosition> wpL = findTerms();
		byte[] section = cutSection (wpL, sectionSize);
		FilterObjectFactory.getInstance().putWordPosition(wpL);
		return section;
	}
	
	public int[] mark(int sectionSize) {
		if ( null == this.bContent) return null;
		List<WordPosition> wpL = findTerms();
		int[] marks = markSection(wpL, sectionSize);
		FilterObjectFactory.getInstance().putWordPosition(wpL);
		return marks;
	}	

	/**
	 * Fins all position of occurances of the supplied words
	 * @return	Found word positions
	 */
	public List<WordPosition> findTerms() {
		
		if( null == this.bContent) return null;
		int wordCount = this.wsize.length;
		int wi = 0;
		//byte bbyte;
		byte cbyte;
		
		List<WordPosition> posL = null;
		
		for (int ci = this.offset; ci < this.endPos; ci++) {
			cbyte = this.bContent[ci];
			
			//Convert only for english
			if ( cbyte >= 'A' && cbyte <= 'Z') cbyte = (byte) (cbyte + 32);  
			/**
			 * cbyte = (char) bbyte;
			 * cbyte = Character.toLowerCase(cbyte);
			 */ 
			
			int cj = 0;
			for (; cj < WORD_DELIMITERS_LENGTH; cj++) {
				if (cbyte == WORD_DELIMITERS[cj]) break; 
			}
			
			if (cj < WORD_DELIMITERS_LENGTH) continue; //Got a word delimiter
			
			for (wi = 0; wi < wordCount; wi++) {
				if (cbyte != this.bWords[wi][0]) continue; //First character matched with one word. Possible 2 words with same first char
				
				int wj = 1;
				for (; wj < this.wsize[wi]; wj++) {
					if ( (ci + wj) > this.endPos ) break;
					byte lbyte = this.bContent[ci + wj]; 
					if ( lbyte >= 'A' && lbyte <= 'Z') lbyte = (byte) (lbyte + 32);  
					if (this.bWords[wi][wj] != lbyte) break;   
				}
				if (wj < this.wsize[wi]) continue;
				
				/**
				 *The word has matched. Check for the next char to
				 *be a word delimiter from WORD_DELIMITERS  
				 */
				if ( (ci + this.wsize[wi]) > this.endPos ) break;
				
				cbyte = this.bContent[ci + this.wsize[wi]];
				if ( cbyte >= 'A' && cbyte <= 'Z') cbyte = (byte) (cbyte + 32);  
				for (wj = 0; wj < WORD_DELIMITERS_LENGTH; wj++) {
					if (cbyte == WORD_DELIMITERS[wj]) break; 
				}
				
				//	Go to the next word
				if (wj >= WORD_DELIMITERS_LENGTH) continue;  
				
				//Found the word, so add the position
				if ( null == posL) posL = FilterObjectFactory.getInstance().getWordPosition();
				posL.add(new WordPosition(wi, ci, (ci + this.wsize[wi])));
				
				/**
				 * Move the reader till the end of the word and into the space.
				 * The for loop will advance it to the next
				 */
				ci = ci + this.wsize[wi]; 
			}
			
			//	Found a word, just go back to the main loop
			if (wi < wordCount) continue; 
			
			//Skip to the start of the next word
			for (; ci <= this.endPos; ci++) {
				cbyte = this.bContent[ci];
				if ( cbyte >= 'A' && cbyte <= 'Z') cbyte = (byte) (cbyte + 32);  
				for (cj = 0; cj < WORD_DELIMITERS_LENGTH; cj++) {
					if (cbyte == WORD_DELIMITERS[cj]) break; 
				}
				//	Got a word delimiter
				if (cj < WORD_DELIMITERS_LENGTH) break; 
			}
			
		}
		return posL;
	}
	
	/**
	 * Cut the most suitable sections
	 * @param wpL	Multiple sighted word positions
	 * @param sectionSize	The length of the teaser section
	 * @return	The best found section
	 */
	public byte[] cutSection (List<WordPosition> wpL, int sectionSize) {
		int[] section = markSection(wpL, sectionSize);
		if ( null == section) return null;
		sectionSize =  section[1] - section[0];
		byte[] sectionB = new byte[sectionSize];
		System.arraycopy(this.bContent, section[0], sectionB , 0, sectionSize);
		return sectionB;
		
	}
	
	/**
	 * Mark the section which requires to be taken out for the teaser.
	 * This can be used for direct array copy than creating temporary
	 * byte arrays.
	 * @param wpL	Multiple sighted word positions
	 * @param sectionSize	The length of the teaser section
	 * @return int array. 0th Location = start position, 1st Location = end position
	 */
	public int[] markSection (List<WordPosition> wpL, int sectionSize) {
		if ( null == this.bContent) return null;
		if ( null == wpL) return null;
		if ( wpL.size() == 0) return null;

		/**
		 * Find the zone where all matchings are noticed.
		 */
		int matchingWordStart = -1, matchingWordEnd = this.offset + sectionSize;
		for (WordPosition wp : wpL) {
			if ( -1 == matchingWordStart) {
				matchingWordStart = wp.start; 
				matchingWordEnd = wp.end;
				continue;
			}
			if (wp.start < matchingWordStart ) matchingWordStart = wp.start; 
			if (wp.end > matchingWordEnd ) matchingWordEnd = wp.end; 
		}
		
		/**
		 * Divide this section to multiple parts and find the degree of concentration
		 */
		int matchingSectionLen = (matchingWordEnd - matchingWordStart);
		if ( matchingSectionLen == 0) return null;
		if ( matchingSectionLen < sectionSize) matchingSectionLen =  sectionSize;
		int matchingSectionsT = matchingSectionLen / sectionSize;
		int[] matchingSections = new int[matchingSectionsT];
		
		for (WordPosition wp : wpL) {
			for ( int zoneIndex=0; zoneIndex<matchingSectionsT; zoneIndex++) {
				int zoneStart = matchingWordStart+ (zoneIndex * sectionSize);
				int zoneEnd = zoneStart + sectionSize;
				if ( wp.start >= zoneStart && wp.start < zoneEnd) 
					matchingSections[zoneIndex] = matchingSections[zoneIndex] + 1; 
			}
		}
		
		/**
		 * Extract maximum concentration area
		 */
		int maxZoneIndex = 0;
		int maxZoneValue = 0;
		for ( int zoneIndex=0; zoneIndex<matchingSectionsT; zoneIndex++) {
			if ( matchingSections[zoneIndex] > maxZoneValue) {
				maxZoneValue = matchingSections[zoneIndex];
				maxZoneIndex = zoneIndex;
			}
		}
		
		int startPos = matchingWordStart + (maxZoneIndex * sectionSize);
		
		// Give little more scope to find an appropriate matching section
		int halfSection = sectionSize/2;
		int end = startPos; 
		startPos = startPos - halfSection;
		if ( startPos < this.offset) startPos = this.offset; 
		
		/**
		 * Create a starting position from a separator
		 */
		int ci = startPos;
		byte cbyte;
		int cj = 0;
		boolean isFound = false;
		
		for (; ci < end; ci++) {
			cbyte = this.bContent[ci];
			for (cj = 0; cj < LINE_DELIMITERS_LENGTH; cj++) {
				if (cbyte == LINE_DELIMITERS[cj]) {
					isFound = true; break;
				} 
			}
			if (isFound) {
				ci++; break;
			}
		}
		
		if ( isFound) {
			startPos = ci;
		} else {
			for (ci = startPos; ci < end; ci++) {
				cbyte = this.bContent[ci];
				if (cbyte == ' ') {
					startPos = ci++; break; 
				}
			}
		}
		
		end = startPos + sectionSize;
		
		
		/**
		 * Create an ending position away from a separator
		 */
		
		if ( end > this.endPos) end =  this.endPos;

		isFound = false;
		for (ci = end; ci >= startPos; ci--) {
			cbyte = this.bContent[ci];
			for (cj = 0; cj < WORD_DELIMITERS_LENGTH; cj++) {
				if (cbyte == WORD_DELIMITERS[cj]) {
					isFound = true;
					break; 
				}
			}
			if (isFound) break;
		}
		
		end = ci;
		if ( end > this.endPos ) end = this.endPos;
		return new int[]{startPos, end};
	}
	

	/**
	 * Carries sighting information of a word inside the content
	 * @author karan
	 *
	 */
	public static class WordPosition {
		
		/**
		 * Query keyword position E.g. (abinash karan hbase = 0,1,2)
		 */
		public int index;
		
		/**
		 * Start position of the word in the given corpus
		 */
		public int start;
		
		/**
		 * End position start position + word length
		 */
		public int end;
		
		/**
		 * Default constrctor
		 * @param index	Query keyword position
		 * @param start	Start position of the word
		 * @param end	End position
		 */
		public WordPosition(int index, int start, int end) {
			this.index = index;
			this.start = start;
			this.end = end;
		}
		
		@Override
		public String toString() {
			return "index:" + index + ", start:" + start + ", end:" + end;
		}
	}
}