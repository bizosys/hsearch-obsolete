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

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.util.StringUtils;

/**
 * Each word stores as a term inside the index.
 * @author karan
 */
public class Term {
	
	public static Character TERMLOC_URL = 'U';
	public static Character TERMLOC_SUBJECT = 'S';
	public static Character TERMLOC_BODY = 'B';
	public static Character TERMLOC_META = 'M';
	public static Character TERMLOC_XML = 'X';
	public static Character TERMLOC_KEYWORD = 'K';
	
	public static String NO_TERM_TYPE = "";
	public static String TERMTYPE_ACRONUM = "ACR";
	public static String TERMTYPE_DATE = "DATE";
	public static String TERMTYPE_EMAIL = "MAIL";
	public static String TERMTYPE_ID = "ID";
	public static String TERMTYPE_URL = "URL";
	public static String TERMTYPE_NOUN = "NAME";
	public static String TERMTYPE_PHONE = "PHONE";
	public static String TERMTYPE_LINKTEXT = "LNKTXT";
	public static String TERMTYPE_MIME = "MM";
	
	/**
	 * This is the position from which we use position jump
	 * to calculate the position. 
	 */
	public static int POSITION_JUMP_FROM = 65000;
	
	/**
	 * This is the serial position of document in the bucket.
	 * A bucket will have capability to store till 65536 documents
	 */
	private short docPos = Short.MIN_VALUE;
	
	/**
	 * The document type (Variation 256)
	 * Now we can support total 256 types of document.
	 * We can map one ID for different document type. 
	 * This will later can be filtered reading the meta fields 
	 * (Low probability of clashing) 
	 */
	private byte docTypeCode = Byte.MIN_VALUE;
	
	/**
	 * The Term type (Variation 256)
	 * Now we can support total 256 types of term types.
	 * This is OK as we can map multiple types to same id
	 * avoiding duplication in docType level (Low probability of clashing) 
	 */
	private byte termTypeCode = Byte.MIN_VALUE ;
	
	/**
	 * Term Weight will be from 0-256
	 */
	private byte weight = 0;
	
	/**
	 * Position of term in the document
	 */
	private short termPos = Short.MIN_VALUE;
	
	/**
	 * The frequency of term in the document
	 */
	private byte termFreq = 1;

	
	/**
	 * Intermediate computation fields
	 */
	public String term;
	public String termType;
	public Character sightting;
	
	/**
	 * A term weight assigned based on the type code
	 * Weight gets assigned from 0-100
	 */
	private byte typeCodeWeight = 0;

	/**
	 * Rest 28 houses are for frequencies, It follows an exponential decay function with
	 * max out at 10. 
	 * */
	private byte freqWeight = 0;
	
	public Term() {
	}
	
	/**
	 * The stored term
	 * @param docPos
	 * @param docTypeCode
	 * @param termTypeCode
	 * @param weight
	 * @param termPos
	 * @param termFreq
	 * @throws ApplicationFault
	 */
	public Term(short docPos, byte docTypeCode, byte termTypeCode, 
		byte weight, int termPos, byte termFreq ) {
		this.docPos = docPos;
		this.docTypeCode = docTypeCode;
		this.termTypeCode = termTypeCode;
		this.weight = weight;
		this.termPos = setTermPos(termPos);
		this.termFreq = termFreq;
	}
	
	/**
	 * 
	 * @param term	Text term
	 * @param sightting	Sightting location
	 * @param termType	Term type
	 * @param termPos	term position
	 * @throws ApplicationFault
	 * @throws SystemFault
	 */
	public Term(String tenant, String term, Character sightting, 
		String termType, Integer termPos ) throws ApplicationFault, SystemFault {
		
		if ( StringUtils.isEmpty(term) ) return;
		
		this.term = term;
		if ( null != termType) {
			if ( termType.length() > 24 ) {
				IndexLog.l.warn("The Term Type is Too Long:" + termType + "\nTenant=" + tenant);
				termType = null;
			}
		}
		
		this.termType = termType;
		
		if ( null != termType ) {
			this.termTypeCode = TermType.getInstance(true).
				getTypeCode(tenant, termType);
			
			this.typeCodeWeight = WeightType.getInstance(true).
				getTypeCode(tenant, termType);
		}
		this.termFreq = 1;
		this.freqWeight = 18;
		this.sightting = sightting;
		this.termPos = 	setTermPos(termPos);
	}
	
	public Term(String term, Character sightting, 
		byte termTypeCode, Integer termPos ) {
			
		if ( StringUtils.isEmpty(term) ) return;
		
		this.term = term;
		this.termTypeCode = termTypeCode;
		this.sightting = sightting;
		this.termPos = 	setTermPos(termPos);
	}
	
	public Term(String term, Character sightting, 
		byte termTypeCode, Integer termPos, short docPos, byte termWeight ) {
		
		this(term,sightting,termTypeCode,termPos);
		this.setDocumentPosition(docPos);
		this.setTermWeight(termWeight);
	}	
	
	
	public void resetTerm(String term) {
		this.docPos = Short.MIN_VALUE;
		this.docTypeCode = Byte.MIN_VALUE;
		this.termTypeCode = Byte.MIN_VALUE ;
		this.termTypeCode = Byte.MIN_VALUE ;
		this.weight = Byte.MIN_VALUE;
		this.termPos = Short.MIN_VALUE;
	}
	
	/**
	 * This will be from -1 till 65530
	 * 32232 + Increment 1 for each 100000 (This is After 65000)  
	 * @param termPos	actural term position
	 * @return	Loss approximate term position (short)
	 */
	public short setTermPos(int termPos) {
		if ( termPos < 65000 ) {
			short termPosCur = new Integer(termPos).shortValue();
			return (short)(Short.MIN_VALUE + termPosCur + 1);
		}
		
		int jump  =  (termPos - 65000) / 100000;
		return (short)( 32232 + jump );
	}
	
	public int getTermPos(short termPos) {
		
		if ( termPos <= 32232 ) {
			return ( (-1 * Short.MIN_VALUE) + termPos - 1);
		}
		
		int jump = (termPos - 32232) * 100000;
		return 65000 + jump ;
	}
	
	/**
	 * We are discounting the term count. Rather we are counting
	 * the sighting location for merging.
	 * @param term
	 */
	public boolean merge(Term term) {
		
		/**
		 * Not the term from same document
		 */
		if ( this.docPos != term.docPos) return false;
		if ( -32768 == term.docPos) return false; 	
		
		/*
		 * Term repetition in the same document 
		 */
		if ( term.weight > this.weight) {
			this.weight = term.weight;
			this.sightting = term.sightting;
			if ( -1 != term.termPos ) this.termPos = term.termPos;
		}
		
		if (term.typeCodeWeight > typeCodeWeight) {
			this.termType = term.termType;
			this.typeCodeWeight = term.typeCodeWeight;
		}
		int totalFreq = this.termFreq + term.termFreq;
		if (totalFreq > Byte.MAX_VALUE) this.termFreq = Byte.MAX_VALUE;
		else this.termFreq = (byte) totalFreq;
		setTermFrequency(this.termFreq);
		return true;
	}
	
	public short getDocumentPosition() {
		return this.docPos;		
	}
	
	public void setDocumentPosition(short pos) {
		this.docPos = pos;		
	}
	
	public byte getDocumentTypeCode() {
		return this.docTypeCode;		
	}
	
	public void setDocumentTypeCode(byte type) {
		this.docTypeCode = type;
	}

	public byte getTermTypeCode() {
		return this.termTypeCode;		
	}
	
	public void setTermTypeCode(byte type) {
		this.termTypeCode = type;
		
	}

	public short getTermPosition() {
		return this.termPos;		
	}
	
	public void setTermWeight(short termPos ) {
		this.termPos = termPos;
	}
	
	public byte getTermWeight() {
		return (byte) ( this.weight + typeCodeWeight + freqWeight);		
	}
	
	public void setTermWeight(byte weight) {
		this.weight = weight;
	}
	
	public byte getTermFrequency() {
		return this.termFreq;	
	}
	
	public void setTermFrequency(byte termFreq) {
		if ( termFreq < 1) this.termFreq = 1;
		else this.termFreq = termFreq;
		
		switch ( this.termFreq ) {
			case 1:
				this.freqWeight = 18; break;
			case 2:
				this.freqWeight = 20; break;
			case 3:
				this.freqWeight = 21; break;
			case 4:
				this.freqWeight = 22; break;
			case 5:
				this.freqWeight = 23; break;
			case 6:
				this.freqWeight = 24; break;
			case 7:
				this.freqWeight = 25; break;
			case 8:
				this.freqWeight = 26; break;
			case 9:
				this.freqWeight = 27; break;
			default :
				this.freqWeight = 28; break;
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Term :" ).append(term);
		sb.append(" , Doc Pos :" ).append(docPos);
		sb.append(" , Doc Type :" ).append(docTypeCode);
		sb.append(" , Term Pos :" ).append(termPos);
		if ( null != termType ) sb.append(" , Term Type :" ).append(termType);
		sb.append(" , Term Freq :" ).append(termFreq);
		sb.append(" , Term Weight :" ).append(weight + "/" + typeCodeWeight + "/" + freqWeight);
		return sb.toString();
	}	
}
