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

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang.StringEscapeUtils;

import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.filter.IStorable;
import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.hsearch.util.DataConstants;
import com.bizosys.oneline.util.StringUtils;

/**
 * It Stores meta information about the document.
 * These meta section helps on dynamic filteration as well as ranking
 * during searching mechanism.
 * An empty meta is currently only 6 byte length.
 * @author karan
 *
 */
public class DocMeta implements IStorable, IDimension {
	
	/**
	 * The state of the docucment (Applied, Processed, Active, Inactive)
	 */
	public String state = null;
	
	/**
	 * Just the Organization Unit (HR, PRODUCTION, SI)
	 * If there are multi level separate it with \ or .
	 */
	public String team = null;

	/**
	 * Northing of a place
	 */
	public Float northing = 0.0f;

	/**
	 * Eastering of a place
	 */
	public Float eastering = 0.0f;

	/**
	 * The Geo House.
	 */
	public String geoHouse = null;

	/**
	 * Document weight : Integer which biases the ranking algorithm.
	 * Document weight is lifted based on it's depth, source 
	 * A home page will have more weight than the deeper location.
	 * documents from Intel page will have more weight 
	 * This could be manually increased to influence the ranking mechanism 
	 */
	public int weight = 0;

	/**
	 * Document Type 
	 * Table Name / File Extension / Dna Name
	 */
	public String docType = null;

	/**
	 * These are author keywords or meta section of the page
	 */
	public String tags = null;

	/**
	 * These are user keywords formed from the search terms
	 */
	public String socialText = null;

	
	/**
	 * Which date the document is created. 
	 */
	public Date createdOn = null;

	/**
	 * Which date the document is last updated. 
	 */
	public Date modifiedOn = null;
	
	/**
	 * Till what date this document is valid
	 */
	public Date validTill = null;
	
	/**
	 * From which IP address is this document created. 
	 * This is specially for machine proximity ranking. 
	 */
	public int ipHouse = 0;

	/**
	 * High Security setting. During high security, 
	 * the information kept encrypted. 
	 */
	public boolean securityHigh = false;

	
	/**
	 * By default the sentiment is positive. 
	 */
	public boolean sentimentPositive = true;
	
	public Locale locale = Locale.ENGLISH;
	
	/**
	 * Default Constructor
	 *
	 */
	public DocMeta() {
	}
	
	public DocMeta(HDocument hdoc) {
		if ( null != hdoc.tags) {
			this.tags  = StringUtils.listToString(hdoc.tags, 
					DataConstants.TAG_SEPARATOR_STORED) ;
			this.tags = StringEscapeUtils.escapeXml(this.tags);
		}
			
		this.createdOn = hdoc.createdOn;
		this.modifiedOn = hdoc.modifiedOn;
		this.validTill = hdoc.validTill;
		this.docType = hdoc.docType;
		
		if ( null != hdoc.eastering) this.eastering = hdoc.eastering;
		if ( null != hdoc.northing ) this.northing = hdoc.northing;
		if ( null != hdoc.team ) this.team = hdoc.team;
		if ( null != hdoc.socialText) 
			this.socialText = StringUtils.listToString(hdoc.socialText, DataConstants.TAG_SEPARATOR_STORED);
		this.securityHigh = hdoc.securityHigh;
		this.sentimentPositive = hdoc.sentimentPositive;
		if ( null != hdoc.state) this.state = hdoc.state;
		this.weight = hdoc.weight;
		if ( null != hdoc.locale) this.locale = hdoc.locale;
	}	
	
	public DocMeta(byte[] bytes) {
		fromBytes(bytes,0);
	}
	
	/**
	 * Read the meta information from the byte array.
	 * Deserialize and initiate
	 * @param bytes : Serialized bytes
	 * @param pos	: Position from which to read the data section
	 */
	public DocMeta(byte[] bytes, int pos) {
		fromBytes(bytes, pos);
	}

	public int fromBytes(byte[] bytes, int pos ) {
		byte docTypeLen = bytes[pos];
		pos++;
		if ( 0 != docTypeLen) {
			byte[] docTypeB = new byte[docTypeLen];
			System.arraycopy(bytes, pos, docTypeB, 0, docTypeLen);
			this.docType = Storable.getString(docTypeB);
			pos = pos + docTypeLen;
		}
		
		byte stateLen = bytes[pos];
		pos++;
		if ( 0 != stateLen) {
			byte[] stateB = new byte[stateLen];
			System.arraycopy(bytes, pos, stateB, 0, stateLen);
			this.state = Storable.getString(stateB);
			pos = pos + stateLen;
		}
		
		byte orgUnitLen = bytes[pos];
		pos++;
		if ( 0 != orgUnitLen) {
			byte[] orgUnitB = new byte[orgUnitLen];
			System.arraycopy(bytes, pos, orgUnitB, 0, orgUnitLen);
			this.team = Storable.getString(orgUnitB);
			pos = pos + orgUnitLen;
		}
		
		byte geoHouseLen = bytes[pos];
		pos++;
		if ( 0 != geoHouseLen) {
			byte[] geoHouseB = new byte[geoHouseLen];
			System.arraycopy(bytes, pos, geoHouseB, 0, geoHouseLen);
			this.geoHouse = Storable.getString(geoHouseB);
			pos = pos + geoHouseLen;
		}
		
		byte flag_1B = bytes[pos++];
		boolean[] flag_1 = Storable.byteToBits(flag_1B);
		
		byte flag_2B = bytes[pos++];
		boolean[] flag_2 = Storable.byteToBits(flag_2B);
		
		int bitPos = 0;
		if ( flag_1[bitPos++]) {
			this.eastering = Float.intBitsToFloat(Storable.getInt(pos, bytes));
			pos = pos+ 4;
		}
		
		if ( flag_1[bitPos++]) {
			this.northing = Float.intBitsToFloat(Storable.getInt(pos, bytes));
			pos = pos+ 4;
		}
		
		if ( flag_1[bitPos++]) {
			this.weight = Storable.getInt(pos, bytes);
			pos = pos+ 4;
		}
		
		if ( flag_1[bitPos++]) {
			this.ipHouse = Storable.getInt(pos, bytes);
			pos = pos+ 4;
		}
		
		this.securityHigh = flag_1[bitPos++];
		this.sentimentPositive = flag_1[bitPos++];
		
		if (flag_1[bitPos++]) {
			short len = Storable.getShort(pos, bytes);
			pos = pos + 2;
			byte[] tagsB = new byte[len];
			System.arraycopy(bytes, pos, tagsB, 0, len);
		    this.tags = Storable.getString(tagsB);
			pos = pos + tagsB.length;
		}
		
		if (flag_1[bitPos++]) {
			short len = Storable.getShort(pos, bytes);
			pos = pos + 2;
			byte[] socialTextB = new byte[len];
			System.arraycopy(bytes, pos, socialTextB, 0, len);
		    this.socialText = Storable.getString(socialTextB);
			pos = pos + socialTextB.length;
		}
		
		bitPos = 0;
		if (flag_2[bitPos++]) {
			this.createdOn = new Date(Storable.getLong(pos, bytes));
			pos = pos+ 8;
		}
		
		if (flag_2[bitPos++]) {
			this.modifiedOn = new Date(Storable.getLong(pos, bytes));
			pos = pos+ 8;
		}
		
		if (flag_2[bitPos++]) {
			this.validTill = new Date(Storable.getLong(pos, bytes));
			pos = pos+ 8;
		}
		return pos;
	}
	
	/**
	 * Filteration criteria
	 */
	public boolean checkActive(Date fromDate, Date toDate) {
		return ( (this.modifiedOn.after(fromDate)) && 
			this.modifiedOn.before(toDate)) ;
	}
	
	/**
	 * Returns all the necessary fields for processing.
	 * orgUnit is treated specially. It goes in a column
	 * This helps to search just on orgUnit fields and then
	 * retrieve documents.
	 * 
	 *  It stores type.. If the type is * means matches all
	 *  
	 */
	public byte[] toBytes() {
		byte docTypeLen = (byte) 0;
		byte[] docTypeB = null;
		if ( null != this.docType) {
			docTypeB = Storable.putString(this.docType);
			docTypeLen = (byte) docTypeB.length;
		}
		
		byte stateLen = (byte) 0;
		byte[] stateB = null;
		if ( null != this.state) {
			stateB = Storable.putString(this.state);
			stateLen = (byte) stateB.length;
		}
		
		byte orgUnitLen = (byte) 0;
		byte[] orgUnitB = null;
		if ( null != this.team) {
			orgUnitB = Storable.putString(this.team);
			orgUnitLen = (byte) orgUnitB.length;
		}
		
		byte geoHouseLen = (byte) 0;
		byte[] geoHouseB = null;
		if ( null != this.geoHouse) {
			geoHouseB = Storable.putString(this.geoHouse);
			geoHouseLen = (byte) geoHouseB.length;
		}
		
		boolean isNorthing = false;
		byte[] northingB = null;
		if ( this.northing != 0.0f) {
			isNorthing = true;
			northingB = Storable.putInt(Float.floatToIntBits(this.northing));
		}

		boolean isEastering = false;
		byte[] easteringB = null;
		if ( this.eastering != 0.0f) {
			isEastering = true;
			easteringB = Storable.putInt(Float.floatToIntBits(this.eastering));
		}
		
		boolean isWeight = false;
		byte[] weightB = null;
		if ( this.weight != 0) {
			isWeight = true;
			weightB = Storable.putInt(this.weight);
		}
		
		boolean isIpHouse = false;
		byte[] iphouseB = null;
		if ( this.ipHouse != 0) {
			isIpHouse = true;
			iphouseB = Storable.putInt(this.ipHouse);
		}
		
		boolean isTags = false;
		byte[] tagsB = null;
		if ( null != this.tags ) {
			isTags = true;
			tagsB = Storable.putString(this.tags);
		}
		
		boolean isSocialText = false;
		byte[] socialTextB = null;
		if ( null != this.socialText ) {
			isSocialText = true;
			this.socialText = this.socialText.toLowerCase();
			socialTextB  = Storable.putString(this.socialText);
		}
		
		boolean isBornOn = false;
		byte[] bornOnB = null;
		if ( null != this.createdOn) {
			isBornOn = true;
			bornOnB = Storable.putLong(this.createdOn.getTime());
		}
		
		boolean isModifiedOn = false;
		byte[] modifiedOnB = null;
		if ( null != this.modifiedOn) {
			isModifiedOn = true;
			modifiedOnB = Storable.putLong(this.modifiedOn.getTime());
		}
		
		boolean isDeathOn = false;
		byte[] deathOnB = null;
		if ( null != this.validTill) {
			isDeathOn = true;
			deathOnB = Storable.putLong(this.validTill.getTime());
		}
		
		byte flag_1 = Storable.bitsToByte(new boolean[] {
			isEastering, isNorthing, isWeight, isIpHouse, securityHigh, sentimentPositive, isTags, isSocialText});
		
		byte flag_2 = Storable.bitsToByte(new boolean[] {
			isBornOn, isModifiedOn, isDeathOn, false, false, false, false, false});
		
		int totalBytes = 1 /** docTypeLen */ + 
			1 /** stateLen */ + 1 /** orgUnitLen */ + 1 /** geoHouseLen */ +
			1 /** dataPresence */ + 1  /** timePresence */ + 
			docTypeLen + stateLen + orgUnitLen + geoHouseLen;
		if ( isEastering) totalBytes = totalBytes + 4;
		if ( isNorthing ) totalBytes = totalBytes + 4;
		if ( isWeight  ) totalBytes = totalBytes + 4;
		if ( isIpHouse  ) totalBytes = totalBytes + 4;
		if ( isTags  ) totalBytes = totalBytes + tagsB.length + 2;
		if ( isSocialText ) totalBytes = 
			totalBytes + socialTextB.length + 2;
			
		if ( isBornOn ) totalBytes = totalBytes + 8;
		if ( isModifiedOn ) totalBytes = totalBytes + 8;
		if ( isDeathOn ) totalBytes = totalBytes + 8;
		
		/**
		 * Writing Start
		 */
		byte[] bytes = new byte[totalBytes];
		int pos = 0;
		
		bytes[pos++] = docTypeLen;
		if ( 0 != docTypeLen)
			System.arraycopy(docTypeB, 0, bytes, pos, docTypeLen);
		pos = pos + docTypeLen;
		
		bytes[pos++] = stateLen;
		if ( 0 != stateLen)
			System.arraycopy(stateB, 0, bytes, pos, stateLen);
		pos = pos + stateLen;
		
		bytes[pos++] = orgUnitLen;
		if ( 0 != orgUnitLen)
			System.arraycopy(orgUnitB, 0, bytes, pos, orgUnitLen);
		pos = pos + orgUnitLen;
		
		bytes[pos++] = geoHouseLen;
		if ( 0 != geoHouseLen)
			System.arraycopy(geoHouseB, 0, bytes, pos, geoHouseLen);
		pos = pos + geoHouseLen;
		
		bytes[pos] = flag_1;
		pos++;
		
		bytes[pos] = flag_2;
		pos++;
		
		if ( isEastering) {
			System.arraycopy(easteringB, 0, bytes, pos, 4);
			pos = pos+ 4;
		}
		
		if ( isNorthing ) {
			System.arraycopy(northingB, 0, bytes, pos, 4);
			pos = pos+ 4;
		}
		
		if (isWeight) {
			System.arraycopy(weightB, 0, bytes, pos, 4);
			pos = pos+ 4;
		}
		
		if ( isIpHouse) {
			System.arraycopy(iphouseB, 0, bytes, pos, 4);
			pos = pos+ 4;
		}
		
		if (isTags) {
			System.arraycopy(Storable.putShort((short)tagsB.length), 0, bytes, pos, 2);
			pos = pos + 2;
			System.arraycopy(tagsB, 0, bytes, pos, tagsB.length);
			pos = pos+ tagsB.length;
		}
		
		if (isSocialText) {
			System.arraycopy(Storable.putShort((short)socialTextB.length), 0, bytes, pos, 2);
			pos = pos + 2;
			System.arraycopy(socialTextB, 0, bytes, pos, socialTextB.length);
			pos = pos+ socialTextB.length;
		}
		
		if (isBornOn) {
			System.arraycopy(bornOnB, 0, bytes, pos, 8);
			pos = pos+ 8;
		}
		
		if (isModifiedOn) {
			System.arraycopy(modifiedOnB, 0, bytes, pos, 8);
			pos = pos+ 8;
		}
		
		if(isDeathOn) {
			System.arraycopy(deathOnB, 0, bytes, pos, 8);
			pos = pos+ 8;
		}
		
		return bytes;
	}

	/**
	 * Cleans up the entire set and make it available for reuse.
	 */
	public void cleanup() {
		this.state = null;
		this.team = null;
		this.northing = 0.0f;
		this.eastering = 0.0f;
		this.weight = 0;
		this.docType = null;
		this.securityHigh = false;
		this.tags = null;
		this.socialText = null;
		this.createdOn = null;
		this.modifiedOn = null;
		this.validTill = null;
		this.ipHouse = 0;
		this.geoHouse = null;
	}
	
	@Override
	public String toString() {
		StringWriter writer = new StringWriter();
		try { 
			toXml(writer);
			writer.close();
			return writer.toString();
			//   Closing a StringWriter has no effect.
		} catch (Exception ex) {
			IndexLog.l.fatal(ex);
			return ex.getMessage();
		}
	}
	
	public void toXml(Writer writer) throws IOException {
		writer.append("<meta>");

		if ( StringUtils.isNonEmpty(this.docType) ) 
			writer.append("<type>").append(this.docType).append("</type>");
		if ( 0 != this.weight ) 
			writer.append("<weight>").append(new Integer(this.weight).toString()).append("</weight>");
		if ( null != this.createdOn ) 
			writer.append("<created>").append(this.createdOn.toString()).append("</created>");
		if ( null != this.validTill) 
			writer.append("<validtill>").append(this.validTill.toString()).append("</validtill>");
		if ( StringUtils.isNonEmpty(this.geoHouse) ) 
			writer.append("<geo>").append(this.geoHouse).append("</geo>");
		if ( null != this.modifiedOn) 
			writer.append("<modified>").append(this.modifiedOn.toString()).append("</modified>");
		if ( StringUtils.isNonEmpty(this.team) ) 
			writer.append("<team>").append(this.team).append("</team>");

		if ( null != this.tags) {
			writer.append("<tags>").append(this.tags.replace(
				DataConstants.TAG_SEPARATOR_STORED, DataConstants.TAG_SEPARATOR_SHOWN)).append("</tags>");
		}
		if ( null != this.socialText) {
			writer.append("<social>").append(this.socialText.replace(
					DataConstants.TAG_SEPARATOR_STORED, DataConstants.TAG_SEPARATOR_SHOWN)).append("</social>");
		}
		if ( StringUtils.isNonEmpty(this.state) ) writer.append("<state>").append(this.state).append("</state>");
		writer.append("<secure>");
		if (securityHigh) writer.append("true");
		else writer.append("false");
		writer.append("</secure>");
		if (!sentimentPositive) {
			writer.append("<sentiment>false</sentiment>");
		}
		writer.append("</meta>");
	}

	public void toNVs(List<NV> nvs) {
		nvs.add(new NV(IOConstants.SEARCH_BYTES,IOConstants.META_BYTES, this));
	}
	
	public void addTags(List<String> tagL) {
		if (this.tags == null) {
			this.tags = StringUtils.listToString(tagL, DataConstants.TAG_SEPARATOR_STORED) ; 
		} else {
			this.tags = this.tags + DataConstants.TAG_SEPARATOR_STORED +  
				StringUtils.listToString(tagL, DataConstants.TAG_SEPARATOR_STORED) ;
		}
	}

	public List<String> getTags() {
		if ( null == tags) return null;
		return StringUtils.fastSplit(tags, DataConstants.TAG_SEPARATOR_STORED);
	}
	
	
	public void addSocialText(List<String> socialText) {
		if (this.socialText == null) {
			this.socialText = StringUtils.listToString(socialText, DataConstants.TAG_SEPARATOR_STORED) ; 
		} else {
			this.socialText = this.socialText + DataConstants.TAG_SEPARATOR_STORED +  
				StringUtils.listToString(socialText, DataConstants.TAG_SEPARATOR_STORED) ;
		}
	}

	public List<String> getSocialText() {
		if ( null == socialText) return null;
		return StringUtils.fastSplit(socialText, DataConstants.TAG_SEPARATOR_STORED);
	}
}
