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

package com.bizosys.hsearch.dictionary;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import com.bizosys.oneline.util.StringUtils;

import com.bizosys.hsearch.filter.IStorable;
import com.bizosys.hsearch.filter.Storable;

/**
 * Represents an entry in the dictionary
 * @author Abinasha karana
 */
public class DictEntry implements IStorable{
		
	public static final String TYPE_SEPARATOR = "\t";
	
	/**
	 * The stemmed word
	 */
	public String word = null;
	
	/**
	 * The word type
	 */
	public String type = null;
	
	/**
	 * Number of documents in which this word is sighted
	 */
	public int frequency = 1;
	
	/**
	 * Synonums of this word
	 */
	public String related = null;
	
	/**
	 * The original unstemmed word
	 */
	public String detail = null;
	
	private int pos = 0;
	
	/**
	 * Private Default Constructor 
	 */
	private DictEntry(){}
	
	public DictEntry ( byte[] value) {
		fromBytes(value,0);
	}
	/**
	 * Constructor, Initialize by deserializing the stored bytes
	 * @param value
	 */
	public DictEntry ( byte[] value, int bytePos) {
		fromBytes(value, bytePos);
	}

	/**
	 * Constructor
	 * @param fldWord	The stemmed word
	 */
	public DictEntry(String fldWord) {
		this.word = fldWord;
	}
	
	/**
	 * Constructor
	 * @param fldWord	The stemmed word
	 * @param fldType	The word type
	 * @param fldFreq	No. of documents containing this word 
	 * @param related	Synonums of this word
	 * @param fldDetailXml	Detail about this word like the thesaurus heirarchy
	 */
	public DictEntry(String fldWord, String fldType, 
		int fldFreq, String related, String fldDetailXml) {
		
		this.word = fldWord;
		if ( null != fldType ) this.type = fldType.trim().toLowerCase();
		this.frequency = fldFreq;
		this.related = related;
		this.detail = fldDetailXml;
	}

	/**
	 * Constructor
	 * @param fldWord	The stemmed word
	 * @param fldType	The word type
	 * @param fldFreq	No. of documents containing this word 
	 */
	public DictEntry(String fldWord, String fldType, Integer fldFreq ) {
		this.word = fldWord;
		if ( null != fldType ) this.type = fldType.trim().toLowerCase();
		this.frequency = fldFreq;
	}
	
	/**
	 * Add synonums word. Add all synonums in a comma separated way.
	 * @param related	Related words
	 */
	public void addRelatedWord(String related) {
		if  (DictLog.l.isDebugEnabled()) DictLog.l.debug(" Related " + related);
		this.related = related;
	}
	
	/**
	 * Serialize the document entry
	 */
	public byte[] toBytes() {
		
		byte[] fldWordB = ( null == word) ? null : Storable.putString(word);
		byte[] fldTypeB = ( null == type) ? null : Storable.putString(type);
		byte[] fldFreqB = Storable.putInt(frequency);
		byte[] fldRelatedB = ( null == related) ? null : Storable.putString(related);
		byte[] fldDetailXmlB = ( null == detail) ? null : Storable.putString(detail);
		
		int fldWordLen = ( null == fldWordB) ? 0 : fldWordB.length;
		int fldTypeLen = ( null == fldTypeB) ? 0 : fldTypeB.length;
		int fldRelatedLen = ( null == fldRelatedB) ? 0 : fldRelatedB.length;
		int fldDetailXmlLen = ( null == fldDetailXmlB) ? 0 : fldDetailXmlB.length;
		
		int totalBytes = fldWordLen + fldTypeLen + 
			fldFreqB.length + fldRelatedLen + fldDetailXmlLen;
		
		byte[] fldWordLenB = Storable.putShort((short) fldWordLen);
		byte[] fldTypeLenB = Storable.putShort((short) fldTypeLen);
		byte[] fldRelatedLenB = Storable.putShort((short) fldRelatedLen);
		byte[] fldDetailXmlLenB = Storable.putShort((short) fldDetailXmlLen);
		
		byte[] value = new byte[totalBytes + 8]; 
		int pos = 0;
		
		System.arraycopy(fldWordLenB, 0, value, pos, 2);
		pos = pos + 2;
		if ( 0 != fldWordLen) {
			System.arraycopy(fldWordB, 0, value, pos, fldWordLen);
			pos = pos + fldWordLen;
		}
		
		System.arraycopy(fldTypeLenB, 0, value, pos, 2);
		pos = pos + 2;
		if ( 0 != fldTypeLen) {
			System.arraycopy(fldTypeB, 0, value, pos, fldTypeLen);
			pos = pos + fldTypeLen;
		}
		
		System.arraycopy(fldFreqB, 0, value, pos, fldFreqB.length);
		pos = pos + fldFreqB.length;

		System.arraycopy(fldRelatedLenB, 0, value, pos, 2);
		pos = pos + 2;
		if ( 0 != fldRelatedLen) {
			System.arraycopy(fldRelatedB, 0, value, pos, fldRelatedLen);
			pos = pos + fldRelatedLen;
		}
		
		System.arraycopy(fldDetailXmlLenB, 0, value, pos, 2);
		pos = pos + 2;
		if ( 0 != fldDetailXmlLen) {
			System.arraycopy(fldDetailXmlB, 0, value, pos, fldDetailXmlLen);
			pos = pos + fldDetailXmlLen;
		}
		return value;
	}
	
	
	public int fromBytes(byte[] data, int readPos) {
		this.pos = readPos;
		short fldWordLen = Storable.getShort(pos, data);
		pos = pos + 2;
		if ( 0 != fldWordLen) {
			byte[] fldWordB = new byte[fldWordLen];
			System.arraycopy(data, pos, fldWordB, 0, fldWordLen);			
			this.word = Storable.getString(fldWordB);
			pos = pos + fldWordLen;
		}
		
		short fldTypeLen = Storable.getShort(pos, data);
		pos = pos + 2;
		if ( 0 != fldTypeLen) {
			byte[] fldTypeB = new byte[fldTypeLen];
			System.arraycopy(data, pos, fldTypeB, 0, fldTypeLen);
			this.type = Storable.getString(fldTypeB);
			pos = pos + fldTypeLen;
		}
		
		this.frequency = Storable.getInt(pos, data);
		pos = pos + 4;
		
		short fldRelatedLen = Storable.getShort(pos, data);
		pos = pos + 2;
		if ( 0 != fldRelatedLen) {
			byte[] fldRelatedB = new byte[fldRelatedLen];
			System.arraycopy(data, pos, fldRelatedB, 0, fldRelatedLen);			
			this.related = Storable.getString(fldRelatedB);
			pos = pos + fldRelatedLen;
		}

		short fldDetailXmlLen = Storable.getShort(pos, data);
		pos = pos + 2;
		if ( 0 != fldDetailXmlLen) {
			byte[] fldDetailXmlB = new byte[fldDetailXmlLen];
			System.arraycopy(data, pos, fldDetailXmlB, 0, fldDetailXmlLen);			
			this.detail = Storable.getString(fldDetailXmlB);
			pos = pos + fldDetailXmlLen;
			
		}
		return pos;
	}	
	
	/**
	 *	Add a type to the word. Example "Bangalore" is a "City" 
	 * @param foundTypes	The word type
	 */
	public void addType(String foundTypes) {
		if ( StringUtils.isEmpty(foundTypes)) return;
		foundTypes = foundTypes.toLowerCase().trim();

		if ( null == this.type) {
			this.type = foundTypes;
			return;
		}
		
		/**
		 * foundTypes=BODY,TITLE and this.type=TITLE
		 */
		List<String> lstType = StringUtils.fastSplit(foundTypes, ',');
		for (String aType : lstType) {
			if ( StringUtils.isEmpty(aType)) return;
			if ( this.type.indexOf(aType) == -1)
				this.type = this.type + TYPE_SEPARATOR + aType;
		}
	}
	
	/**
	 * Get all types associated to this word.
	 * Ex. Hydrogen is a "Molecule" as well as a "Fuel" 
	 * @return	All types
	 */
	public List<String> getTypes() {
	    if (StringUtils.isEmpty(this.type)) return null;
	    StringTokenizer tokenizer = new StringTokenizer (this.type, TYPE_SEPARATOR);
	    List<String> values = new ArrayList<String>();
	    while (tokenizer.hasMoreTokens()) {
	    	String token = tokenizer.nextToken();
	    	if (StringUtils.isEmpty(token)) continue;
	    	values.add(token);
	    }
	    return values;
	}
	
	/**
	 * Forms a XML representation of this entry
	 * @param writer	Writer
	 * @throws IOException	Write exception
	 */
	public void toXml(Writer writer) throws IOException {
		writer.append("<aword>");
		if ( null != this.word ) writer.append("<word>").append(this.word).append("</word>");
		if ( null != this.type ) writer.append("<type>").append(this.type.replace('\t', ',')).append("</type>");
		writer.append("<frequency>").append(new Integer(this.frequency).toString()).append("</frequency>");
		if ( null != this.related ) writer.append("<related>").append(this.related.replace('\t', ',')).append("</related>");
		if ( null != this.detail ) writer.append("<detail>").append(this.detail).append("</detail>");
		writer.append("</aword>");
	}
	
	
	@Override 
	public String toString() {
		StringBuilder sb = new StringBuilder(100);
		if ( null != this.word ) sb.append(" Word:[").append(this.word).append(']');
		if ( null != this.type ) sb.append(" , Type:[").append(this.type.replace('\t', ',')).append(']');
		sb.append(" , Freq:[").append(this.frequency).append(']');
		if ( null != this.related ) sb.append(" , Related:[").append(this.related.replace('\t', ',')).append(']');
		if ( null != this.detail )sb.append(" , Detail:[").append(this.detail).append(']');
		return sb.toString();
	}
}