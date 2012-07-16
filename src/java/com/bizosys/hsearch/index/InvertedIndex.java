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
package com.bizosys.hsearch.index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.util.ObjectFactory;

/**
 * The inverted Index data structure and byte operations.
 * @author karan
 *
 */
public class InvertedIndex {
	private static final byte[] EMPTY_BYTES = new byte[]{};
	public int hash;
	public byte[] dtc; //doc type code
	public byte[] ttc; //term type code
	public byte[] tw; //term weight
	public byte[] termFreq; //term freq
	public short[] termPos; //term pos
	public short[] docPos; //doc pos
	
	public InvertedIndex(int hash, byte[] dtc, byte[] ttc, byte[] tw,  
		byte[] termFreq,short[] termPos,short[] docPos ) {
		
		this.hash = hash;
		this.dtc = dtc;
		this.ttc = ttc;
		this.tw = tw;
		this.termFreq = termFreq;
		this.termPos = termPos;
		this.docPos = docPos;
	}
	

	
	/**
	 * Reads the bytes to reconstruct the Inverted Index
	 * @param bytes	Input bytes
	 * @return	inverted index entries 
	 */
	public static List<InvertedIndex> read(byte[] bytes) {
		
		if ( null == bytes) return null;
		int cursor = 0;
		int bytesT = bytes.length;
		if ( 0 == bytesT) return null;
		
		List<InvertedIndex> invIndex = new ArrayList<InvertedIndex>(); 
		
		while (cursor < bytesT) {
			int hash = Storable.getInt(cursor, bytes);
			cursor = cursor + 4;
			int termsT = (byte) bytes[cursor];
			cursor++;
			if ( -1 == termsT) {
				termsT = Storable.getInt(cursor,bytes );
				cursor = cursor + 4;
			}

			byte[] dtc = new  byte[termsT];
			System.arraycopy(bytes, cursor, dtc, 0, termsT);
			cursor = cursor + termsT;
			
			byte[] ttc = new  byte[termsT];
			System.arraycopy(bytes, cursor, ttc, 0, termsT);
			cursor = cursor + termsT;
			
			byte[] tw = new  byte[termsT];
			System.arraycopy(bytes, cursor, tw, 0, termsT);
			cursor = cursor + termsT;
			
			byte[] tf = null;
			short[] tp = null;
			if ( TermList.termVectorStorageEnabled ) {
				tf = new  byte[termsT];
				System.arraycopy(bytes, cursor, tf, 0, termsT);
				cursor = cursor + termsT;
		
				tp = new  short[termsT];
				for (int i=0; i< termsT; i++) {
					tp[i] = Storable.getShort(cursor, bytes);
					cursor = cursor + 2;
				}
			}

			short[]  dp = new  short[termsT];
			for (int i=0; i< termsT; i++) {
				dp[i] = Storable.getShort(cursor, bytes);
				cursor = cursor + 2;
			}
			InvertedIndex ii = new InvertedIndex(hash,dtc, ttc,tw,tf,tp,dp);
			invIndex.add(ii);
		}
		return invIndex;
	}
	
	/**
	 * Remove the document at the specified position
	 * @param bytes	Input bytes
	 * @param docPos 	Document position
	 * @return	Document purged
	 */
	public static byte[] delete(byte[] bytes, short docPos) {
		
		if ( null == bytes) return null;
		int cursor = 0;
		int bytesT = bytes.length;
		if ( 0 == bytesT) return null;
		
		Map<Integer,Integer> rowcol = new HashMap<Integer,Integer>(); 
		int row = 0;
		int termsT = 0;
		int col = -1;
		short dp;
		while (cursor < bytesT) {
			row++;
			cursor = cursor + 4; //Hash
			termsT = (byte) bytes[cursor];
			cursor++;
			
			if ( -1 == termsT) {
				termsT = Storable.getInt(cursor,bytes );
				cursor = cursor + 4;
			}
			
			cursor = cursor + (termsT * 3); //dtc + ttc + tw
			if ( TermList.termVectorStorageEnabled ) cursor = cursor + (termsT * 3); //tf + tp
			col = Integer.MIN_VALUE;
			for (int i=0; i< termsT; i++) {
				dp = Storable.getShort(cursor, bytes);
				cursor = cursor + 2;
				if ( dp == docPos) {
					cursor = cursor + (termsT - i - 1) * 2; //Remaining bytes
					col = ( termsT == 1 ) ? -1 : i; 
					break;
				}
			}
			if ( Integer.MIN_VALUE != col ) rowcol.put(row,col);
		}
		
		if ( IndexLog.l.isTraceEnabled()) IndexLog.l.trace(
			"InvertedIndex:delete Rows :" + 
			rowcol.values().toString() + 
			"\tCols :" + rowcol.keySet().toString() );
		
		/**
		 * Now cut the actual values
		 */
		cursor = 0; row = 0;
		ByteBuffer bb = ByteBuffer.allocate(bytes.length);
		
		while (cursor < bytesT) {
			row++;
			boolean cutRow = rowcol.containsKey(row);
			if ( cutRow && rowcol.get(row) == -1 ) {
				cursor = cursor + 4; //Hashcode
				termsT = (byte) bytes[cursor++];
				if ( -1 == termsT) {
					termsT = Storable.getInt(cursor,bytes );
					cursor = cursor + 4;
				}
				if ( TermList.termVectorStorageEnabled ) cursor = cursor + termsT * 8; 
				else cursor = cursor + termsT * 5;
				continue;
			}
			
			bb.put(bytes, cursor, 4);
			cursor = cursor + 4;
			termsT = (byte) bytes[cursor++];
			if ( -1 == termsT) {
				bb.put( (byte) -1);
				termsT = Storable.getInt(cursor,bytes );
				if ( cutRow ) bb.putInt(termsT - 1); 
				else bb.put(bytes, cursor, 4);
				cursor = cursor + 4;
			} else {
				if ( cutRow ) bb.put( (byte) (termsT - 1) );
				else bb.put( (byte) (termsT) );
			}
			
			if ( cutRow ) {
				col = rowcol.get(row);
				if ( col != 0 ) bb.put(bytes, cursor, col);
				bb.put(bytes, cursor + col + 1, termsT - col - 1);
				cursor = cursor + termsT;

				//Copy Term Type Code
				if ( col != 0 )  bb.put(bytes, cursor, col);
				bb.put(bytes, cursor + col + 1, termsT - col - 1);
				cursor = cursor + termsT;

				//Copy Term Weight
				if ( col != 0 ) bb.put(bytes, cursor, col);
				bb.put(bytes, cursor + col + 1, termsT - col - 1);
				cursor = cursor + termsT;

				if ( TermList.termVectorStorageEnabled ) {
					//Copy Term Frequency
					if ( col != 0 ) bb.put(bytes, cursor, col);
					bb.put(bytes, cursor + col + 1, termsT - col - 1);
					cursor = cursor + termsT;
					
					//Copy Term Position
					if ( col != 0 ) bb.put(bytes, cursor, (col) * 2 );
					bb.put(bytes, cursor + (col + 1) * 2, (termsT - col - 1) * 2);
					cursor = cursor + termsT * 2;
				} 
				//Copy Doc Position
				if ( col != 0 ) bb.put(bytes, cursor, col * 2 );
				bb.put(bytes, cursor + (col + 1) * 2, (termsT - col - 1) * 2);
				cursor = cursor + termsT * 2;
				
			} else {
				if ( TermList.termVectorStorageEnabled ) {
					bb.put(bytes, cursor, termsT * 8);
					cursor = cursor + termsT * 8; 
				} else {
					bb.put(bytes, cursor, termsT * 5);
					cursor = cursor + termsT * 5; 
				}
			}
		}
		int len = bb.position();
		if ( IndexLog.l.isTraceEnabled() ) IndexLog.l.trace(
			"InvertedIndex : Original / Cut Byte Size =" + bytes.length + "/" + len);
		if ( 0 == len ) return EMPTY_BYTES;
		byte[] deletedB = new byte[len];
		bb.position(0);
		bb.get(deletedB, 0, len);
		bb.clear();
		return deletedB;
	}	
		
	
	/**
	 * Merge the supplied document list with the documents
	 * already present in the bucket.
	 * 
	 * Ignore all the supplied documents while loading from bytes the existing ones
	 * Create the Term List 
	 *
	 */
	public static void merge(byte[] existingB, Map<Integer, List<Term>> lstKeywords) {
		
		if ( null == existingB) return;

		short docPos;
		Set<Short> freshDocs = getFreshDocs(lstKeywords);
		
		if ( IndexLog.l.isDebugEnabled()) {
			for (int hash : lstKeywords.keySet()) {
				IndexLog.l.debug(
					"List : " + hash + " = " + lstKeywords.get(hash).toString());
			}
		}
		
		int bytesT = existingB.length;
		List<Term> priorDocTerms = ObjectFactory.getInstance().getTermList();
		int keywordHash = -1, termsT = -1, shift = 0, pos = 0, readPos=0;
		byte docTyep=0,termTyep=0,termWeight=0,termFreq=0;
		short termPos=0;
		
		if ( IndexLog.l.isDebugEnabled() ) IndexLog.l.debug("TermList Byte Marshalling: bytesT = " + bytesT);
		while ( pos < bytesT) {
			priorDocTerms.clear();
			keywordHash = Storable.getInt(pos, existingB);
			pos = pos + 4;

			/**
			 * Compute number of terms presence.
			 */
			termsT = existingB[pos++];
			if ( -1 == termsT ) {
				termsT =  Storable.getInt(pos, existingB);
				pos = pos + 4;
			} 
			//if ( L.l.isDebugEnabled() ) L.l.debug("termsT:" + termsT + ":" + pos );
			
			/**
			 * Compute Each Term.
			 */
			shift = TermList.TERM_SIZE_NOVECTOR;
			if ( TermList.termVectorStorageEnabled ) shift = TermList.TERM_SIZE_VECTOR;
			for ( int i=0; i<termsT; i++) {
				//if ( IndexLog.l.isDebugEnabled() ) IndexLog.l.debug("pos:" + pos );
				
				readPos = pos + ((shift - 2) * termsT )+ (i * 2);
				docPos = (short) ((existingB[readPos] << 8 ) + 
					( existingB[++readPos] & 0xff ));
				
				if ( freshDocs.contains(docPos)) continue;
				
				docTyep = existingB[pos+i];
				termTyep = existingB[pos + termsT + i];
				termWeight = existingB[pos + (2 * termsT) + i];
				
				if ( TermList.termVectorStorageEnabled ) {
					termFreq = existingB[pos + (3 * termsT) + i];
					readPos = pos + (4 * termsT) + i;
					termPos = (short) ( (existingB[readPos] << 8 ) + 
							( existingB[++readPos] & 0xff ) );
				}
				Term priorTerm = new Term(docPos,docTyep,termTyep,termWeight,termPos,termFreq);
				priorDocTerms.add(priorTerm);
			}

			if ( TermList.termVectorStorageEnabled ) pos = pos + (8 * termsT);
			else pos = pos + (5 * termsT);
			mergePrior(lstKeywords, priorDocTerms, keywordHash);
			ObjectFactory.getInstance().putTermList(priorDocTerms);
		}
	}

	/**
	 * Merge prior documents
	 * @param lstKeywords
	 * @param priorDocTerms
	 * @param keywordHash
	 */
	private static void mergePrior(Map<Integer, List<Term>> lstKeywords,
		List<Term> priorDocTerms, int keywordHash) {
		
		if ( priorDocTerms.size() > 0 ) {
			List<Term> terms = null;
			if ( lstKeywords.containsKey(keywordHash) ) { //This Keyword exists
				terms = lstKeywords.get(keywordHash);
				terms.addAll(priorDocTerms);
			} else {
				List<Term> docTerms = new ArrayList<Term>(priorDocTerms.size());
				docTerms.addAll(priorDocTerms);
				lstKeywords.put(keywordHash, docTerms);
			}

			if ( IndexLog.l.isDebugEnabled()) {
				IndexLog.l.debug("#### KEYWORDS HASH READ START");
				for (int hash : lstKeywords.keySet()) {
					IndexLog.l.debug("Merged : " + hash + " = " + 
						lstKeywords.get(hash).toString());
				}
				IndexLog.l.debug("KEYWORDS HASH READ ENDS ####");
			}
		}
	}
	
	/**
	 * Get the fresh documentrs (The document position are absent)
	 * @param lstKeywords
	 * @return
	 */
	private static Set<Short> getFreshDocs(Map<Integer, List<Term>> lstKeywords) {
		Set<Short> freshDocs = new HashSet<Short>();
		short docPos;
		for (int hash : lstKeywords.keySet()) {
			List<Term> terms = lstKeywords.get(hash);
			for (Term term : terms) {
				docPos = term.getDocumentPosition();
				if ( freshDocs.contains(docPos)) continue;
				freshDocs.add(docPos);
			}
		}
		if ( IndexLog.l.isDebugEnabled() ) 
			IndexLog.l.debug("Fresh Documents:" + freshDocs.toString());
		return freshDocs;
	}

	public String toString() {
		
		StringBuilder sb = new StringBuilder();
		sb.append("Hash: [").append(hash);

		sb.append("]\nDocument Type: [");
		if ( null != dtc) for (byte c : dtc) sb.append(c).append(',');

		sb.append("]\nTerm Type: [");
		if ( null != ttc) for (byte c : ttc) sb.append(c).append(',');

		sb.append("]\nTerm Weight: [");
		if ( null != tw) for (byte w : tw) sb.append(w).append(',');
		
		sb.append("]\nTerm Frequency: [");
		if ( null != termFreq) for (byte tf : termFreq) sb.append(tf).append(',');
		
		sb.append("]\nTerm Position: [");
		if ( null != termPos) for (short tp : termPos) sb.append(tp).append(',');

		sb.append("]\nDocument Position = [");
		if ( null != docPos) for (short dp : docPos) sb.append(dp).append(',');
		sb.append(']');
		
		return sb.toString();
	}	
	
}
