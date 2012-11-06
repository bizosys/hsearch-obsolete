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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.bizosys.hsearch.filter.IStorable;
import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.schema.ILanguageMap;
import com.bizosys.hsearch.util.ObjectFactory;

/**
 * Multiple terms grouped as termlist
 * @author karan
 *
 */
public class TermList implements IStorable {
	
	public static final int TERM_SIZE_VECTOR = 8;

	public static final int TERM_SIZE_NOVECTOR = 5;

	public static boolean termVectorStorageEnabled = false;
	
	/**
	 * Total terms present for this keyword
	 */
	public int totalTerms;
	
	/**
	 * Document type codes in a array for all terms
	 */
	public byte[] docTypesCodes;

	/**
	 * Term type codes in a array for all terms
	 */
	public byte[] termTypeCodes;

	/**
	 * Term weight in a array for all terms
	 */
	public byte[] termWeight;
	
	/**
	 * How many times the term is sightted in the document
	 */
	public byte[] termFreq;
	
	/**
	 * Which location of the document, the term is positioned
	 */
	public short[] termPosition;
	
	/**
	 * This document is at which location of the bucket
	 */
	public short[] docPos;
	
	/**
	 * All terms are listed here. we merge and keep the same term only in the list.
	 */
	public Map<Integer, List<Term>> lstKeywords = null; 
	
	private byte[] existingB = null;

	public TermList() {
	}
	

	public void setExistingBytes(byte[] existingB) {
		this.existingB = existingB;
	}
	
	public void loadTerms(byte[] bytes, Set<Integer> ignoreLocation,
			Byte docType, Byte termType) {
		
		if ( null == bytes) return;
		
		if ( termVectorStorageEnabled ) {
			this.totalTerms = bytes.length / TERM_SIZE_VECTOR;
		} else {
			this.totalTerms = bytes.length / TERM_SIZE_NOVECTOR;
		}
		
		if ( DocumentType.NONE_TYPECODE != docType) {
			byte docTypeCode =docType.byteValue();
			for (int i=0; i<this.totalTerms; i++ ) {
				if ( docTypeCode != bytes[i] ) ignoreLocation.add(i);
			}
		}
		
		if ( TermType.NONE_TYPECODE != termType) {
			byte termTypeCode =termType.byteValue();
			for (int i=0; i<this.totalTerms; i++ ) {
				if ( termTypeCode != bytes[this.totalTerms + i] ) ignoreLocation.add(i);
			}
		}
		int ignoreLocationT = ignoreLocation.size();
		if ( ignoreLocationT == 0) {
			loadTerms(bytes);
			return;
		}
		
		/**
		 * Document types codes
		 */
		int newTotals = this.totalTerms - ignoreLocationT;
		docTypesCodes = new byte[newTotals];
		termTypeCodes = new byte[newTotals];
		this.termWeight = new byte[newTotals];
		if ( termVectorStorageEnabled ) {
			this.termFreq = new byte[newTotals];
			this.termPosition = new short[newTotals];
		}
		this.docPos = new short[newTotals];
		
		int row = 0;
		int shift = 0;
		for (int i=0; i<this.totalTerms; i++ ) {
			if ( ignoreLocation.contains(i)) continue;

			docTypesCodes[row] = bytes[i];
			termTypeCodes[row] = bytes[this.totalTerms + i];
			this.termWeight[row] = bytes[(2 * this.totalTerms) + i];
			if ( termVectorStorageEnabled ) {
				this.termFreq[row] = bytes[(3 * this.totalTerms) + i];
				shift = (this.totalTerms * 4 ) + (i * 2);
				this.termPosition[row] = (short) ((bytes[shift] << 8 ) + ( bytes[shift+1] & 0xff ) );
				shift = (this.totalTerms * 6) + (i * 2);
				this.docPos[row] = (short) ((bytes[shift] << 8 ) + ( bytes[shift+1] & 0xff ) );
			} else {
				shift = (this.totalTerms * 3) + (i * 2);
				this.docPos[row] = (short) ((bytes[shift] << 8 ) + ( bytes[shift+1] & 0xff ) );
			}
			row++;
		}
		
		this.totalTerms = newTotals;
	}
	
	/**
	 * Load by deserializing bytes
	 * @param bytes
	 */
	public void loadTerms(byte[] bytes) {
		if ( null == bytes) return;
		
		int readPosition = 0;
		
		if ( termVectorStorageEnabled ) {
			this.totalTerms = bytes.length / TERM_SIZE_VECTOR;
		} else {
			this.totalTerms = bytes.length / TERM_SIZE_NOVECTOR;
		}
		
		/**
		 * Document types codes
		 */
		docTypesCodes = new byte[this.totalTerms];
		for (int i=0; i<this.totalTerms; i++ ) {
			docTypesCodes[i] = bytes[readPosition++];
		}
		
		/**
		 * Term types codes
		 */
		termTypeCodes = new byte[this.totalTerms];
		for (int i=0; i<this.totalTerms; i++ ) {
			termTypeCodes[i] = bytes[readPosition++];
		}
		
		/**
		 * Term weight
		 */
		this.termWeight = new byte[this.totalTerms];
		for (int i=0; i<this.totalTerms; i++ ) {
			this.termWeight[i] = bytes[readPosition++];
		}
		
		if ( termVectorStorageEnabled ) {
			/**
			 * Term frequency
			 */
			this.termFreq = new byte[this.totalTerms];
			for (int i=0; i<this.totalTerms; i++ ) {
				this.termFreq[i] = bytes[readPosition++];
			}	

			/**
			 * Term Position
			 */
			this.termPosition = new short[this.totalTerms];
			for (int i=0; i<this.totalTerms; i++ ) {
				this.termPosition[i] = 
					(short) ((bytes[readPosition++] << 8 ) + ( bytes[readPosition++] & 0xff ) );
			}	
		}
		
		/**
		 * Document Position
		 */
		this.docPos = new short[this.totalTerms];
		for (int i=0; i<this.totalTerms; i++ ) {
			this.docPos[i] = 
				(short) ((bytes[readPosition++] << 8 ) + ( bytes[readPosition++] & 0xff ) );
		}
	}
	
	/**
	 * Add a keyword. Repetition are taken care
	 * @param aTerm
	 */
	public void add(Term aTerm) {
		if ( null == aTerm) return;

		int keywordHash = aTerm.term.hashCode();			
		if ( null == lstKeywords) {
			lstKeywords = new HashMap<Integer, List<Term>> (ILanguageMap.ALL_COLS.length);
			List<Term> lstTerms = ObjectFactory.getInstance().getTermList();
			lstTerms.add(aTerm);
			lstKeywords.put(keywordHash, lstTerms);
			return;
		}
		
		boolean isMerged = false;
		if ( lstKeywords.containsKey(keywordHash)) {
			//Ids from same document is merged.
			for ( Term existing : lstKeywords.get(keywordHash)) {
				isMerged = existing.merge(aTerm);
				if ( isMerged ) break;
			}
			if ( !isMerged )  lstKeywords.get(keywordHash).add(aTerm);
			
		} else {
			List<Term> terms = new ArrayList<Term>(3);
			terms.add(aTerm);
			lstKeywords.put(keywordHash, terms);
		}
	}
	
	/**
	 * Add a complete list here.
	 * @param anotherList
	 */
	public void add(TermList anotherList) {
		if ( null == anotherList.lstKeywords) return;
		for (List<Term> anotherTerms : anotherList.lstKeywords.values()) {
			for (Term anotherTerm : anotherTerms) {
				this.add(anotherTerm);
			}
		}
	}
	
	/**
	 * Remove all the ids from another which are absent here.
	 * Remove all the ids from here which are absent another
	 * @param another
	 * @return : After intersect has any element left?
	 */
	public boolean intersect(TermList another) {
		
		if ( 0 == this.totalTerms) another.cleanup();
		if ( 0 == another.totalTerms) this.cleanup();

		if ( null == this.docPos) another.cleanup();
		if ( null == another.docPos) this.cleanup();

		if ( 0 == this.totalTerms) return false;
		
		boolean notSubsetting = true;
		short aPos = -1;
		int totalMatching = 0;
		int posT = this.docPos.length;
		
		/**
		 * This is a costlier looking cycle. 
		 * TODO: Evaluate Set to make it faster
		 */
		for (int i=0; i<posT; i++) {
			aPos = this.docPos[i];
			if ( -1 == aPos) continue;
			notSubsetting = true;
			for ( short bPos : another.docPos) {
				if ( aPos == bPos) {
					notSubsetting = false; totalMatching++; break;
				}
			}
			if ( notSubsetting )  this.docPos[i] = -1;
		}
		
		/**
		 * No terms matched
		 */
		if ( 0 == totalMatching) {
			this.cleanup();
			another.cleanup();
			return false;
		}
		
		/**
		 * Set other document positions also as -1
		 * 
		 * This is a costlier looking cycle. 
		 * TODO: Evaluate Set to make it faster
		 */

		posT = another.docPos.length;
		for (int i=0; i<posT; i++) {
			aPos = another.docPos[i];
			if ( -1 == aPos) continue;
			notSubsetting = true;
			
			//Is this existing in other list
			for ( short posAno : this.docPos) {
				if ( aPos == posAno) {
					notSubsetting = false; totalMatching--; break;
				}
			}
			if ( notSubsetting )  another.docPos[i] = -1;
			if ( -1 == totalMatching) break; //Don't process unnecessarily
		}
		return true;
	}
	
	/**
	 * This keeps matching ids only of another termlist
	 * @param another  After subsetting has any element left?
	 */
	public boolean subset(TermList another) {
		if ( 0 == another.totalTerms) this.cleanup();
		if ( null == another.docPos) this.cleanup();
		if ( 0 == this.totalTerms) return false;
		
		short aPos = -1;
		int posT = this.docPos.length;
		boolean eliminate = true;
		boolean noneFound = true;

		for ( int i=0; i<posT; i++) { //This term
			aPos = this.docPos[i];
			if ( -1 == aPos) continue;
			eliminate = true;
			for (short bPos : another.docPos) { //Any presence @ must terms
				if ( -1 == bPos) continue;
				if ( aPos == bPos) {
					eliminate = false;
					noneFound = false;
					break;
				}
			}
			if (eliminate) this.docPos[i] = -1;
		}

		/**
		 * No terms matched
		 */
		if ( noneFound ) {
			this.cleanup(); 
			return false;
		}
		
		return true;
	}
	
	/**
	 * The given document id will be applied to 
	 * @param position
	 */
	public void assignDocPos(int position) {
		if ( null == this.lstKeywords) return;
		short pos = (short) position;
		for (List<Term> terms : lstKeywords.values()) {
			for (Term term : terms) {
				term.setDocumentPosition(pos);
			}
		}
	}
	
	public boolean isExistingUnchanged() {
		if ( null == lstKeywords) return true;
		else return false;
	}
	

	/**
	 * Serialize this
	 * KeywordHash1/byte(SIZE > 256)/Integer(SIZE)/BYTES
	 * KeywordHash2/byte(SIZE)/Integer(SIZE)/BYTES
	 */
	public byte[] toBytes() {
		
		if ( null == lstKeywords) return this.existingB;
		
		InvertedIndex.merge(this.existingB, lstKeywords);
		
		int totalBytes = 0;
		int termsT = 0;
		List<Term> lstTerms  = null;

		for (int hash : lstKeywords.keySet()) {
			totalBytes = totalBytes + 4; /**Keyword Hash*/ 
			lstTerms  = lstKeywords.get(hash);
			termsT = lstTerms.size();
			if ( termsT < Byte.MAX_VALUE) totalBytes++;  /**Low density*/ 
			else totalBytes = totalBytes + 5; /**High density*/
			if ( termVectorStorageEnabled ) { /**Terms*/
				totalBytes = totalBytes + termsT * 8;
			} else {
				totalBytes = totalBytes + termsT * 5;
			}
		} 
		
		byte[] bytes = new byte[totalBytes];
		int pos = 0;
		short tp = 0, dp = 0;

		for (int hash : lstKeywords.keySet()) {

			/**
			 * Add the keyword hash
			 */
			System.arraycopy(Storable.putInt(hash), 0, bytes, pos, 4);
			pos = pos + 4;
			
			/**
			 * Add the total terms
			 */
			lstTerms  = lstKeywords.get(hash);
			termsT = lstTerms.size();
			if ( termsT < Byte.MAX_VALUE) {
				bytes[pos++] = (byte)(termsT);
			} else {
				bytes[pos++] = (byte)(-1);  
				System.arraycopy(Storable.putInt(termsT), 0, bytes, pos, 4);
				pos = pos + 4;
			}
			
			/**
			 * Document types codes
			 */
			for (Term t : lstTerms) {
				bytes[pos++] = t.getDocumentTypeCode();
			}
			
			/**
			 * Term types codes
			 */
			for (Term t : lstTerms) {
				bytes[pos++] = t.getTermTypeCode();
			}
			
			/**
			 * Term weight
			 */
			for (Term t : lstTerms) {
				bytes[pos++] = t.getTermWeight();
			}
			
			if ( termVectorStorageEnabled ) {
				
				/**
				 * Term frequency
				 */
				for (Term t : lstTerms) {
					bytes[pos++] = t.getTermFrequency();
				}		
		
				/**
				 * Term Position
				 */
				for (Term t : lstTerms) {
					tp = t.getTermPosition();
					bytes[pos++] = (byte)(tp >> 8 & 0xff);
					bytes[pos++] = (byte)(tp & 0xff);
				}
			}

			/**
			 * Document Position
			 */
			for (Term t : lstTerms) {
				dp = t.getDocumentPosition();
				bytes[pos++] = (byte)(dp >> 8 & 0xff);
				bytes[pos++] = (byte)(dp & 0xff);
			}
		}
		return bytes;
	}
	
	public int fromBytes(byte[] bytes, int pos) {
		if ( null == bytes ) return pos;
		if ( 0 == pos) {
			this.existingB = bytes;
			return pos;
		}
		
		int size = bytes.length - pos;
		this.existingB = new byte[size];
		System.arraycopy(bytes, pos, this.existingB, 0, size);
		return (pos + size);
	}

	
	/**
	 * Does this list contain the keyword.
	 * @param bytes	Input bytes
	 * @param keywordHash	search word hashcode in bytes
	 * @param pos	starting read position
	 * @return True if matched
	 */
	public static boolean isMatchedTerm(byte[] bytes, 
			byte[] keywordHash, int pos) {
			
			return 	(bytes[pos++] == keywordHash[0]) &&
					(bytes[pos++] == keywordHash[1]) &&
					(bytes[pos++] == keywordHash[2]) &&
					(bytes[pos++] == keywordHash[3]);
	}
	
	public void cleanup() {
		totalTerms = 0;
		this.docTypesCodes = null;
		this.termTypeCodes = null;
		this.termWeight = null;
		this.termFreq = null;
		this.termPosition = null;
		this.docPos = null;
		if (null != lstKeywords) {
			for (List<Term> lt : lstKeywords.values()) {
				if ( null == lt) continue;
				ObjectFactory.getInstance().putTermList(lt);	
			}
			lstKeywords.clear();
		}
		this.existingB = null;
	}
	
	@Override
	public String toString() {
		
		if ( null != this.lstKeywords) {
			StringBuilder sb = new StringBuilder(" TermList : ");
			for (int termHash: this.lstKeywords.keySet()) {
				sb.append(termHash).append(" : ");
				for ( Term aTerm : this.lstKeywords.get(termHash)) {
					sb.append("\n\t\t\t\t\t").append(aTerm.toString());
				}
			}
			return sb.toString();
		} else {
			StringBuilder sb = new StringBuilder("\nTermList Total : ");
			sb.append(totalTerms);
			for ( int i=0; i< totalTerms; i++) {
				sb.append("\nPositions: ").append(this.docPos[i]);
				sb.append(" Weight: ").append(this.termWeight[i]);
			}
			return sb.toString();
		}
		
	}
	
}
