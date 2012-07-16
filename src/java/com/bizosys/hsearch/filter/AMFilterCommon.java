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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Match the Meta and ACL section of document
 * @author karan
 *
 */
public class AMFilterCommon {
	 
	/**
	 * Read access
	 */
	public AccessStorable userAcl;
	
	/**
	 * Check presence inside keywords
	 */
	public byte[] keyword = null;

	/**
	 * Is state matching
	 */
	public byte[] state = null;

	/**
	 * Is team matching
	 */
	public byte[] team = null;

	/**
	 * Created before the Given date
	 */
	public long maxCreationDate = -1;

	/**
	 * Created after the Given date
	 */
	public long minCreationDate = -1;

	/**
	 * Modified before the Given date
	 */
	public long maxModificationDate = -1;

	/**
	 * Modified after the Given date
	 */
	public long minModificationDate = -1;
	
	/**
	 * Bytes array
	 */
	public byte[] bytesA = null;
	 
	/**
	 * Default Constructor
	 *
	 */
	public AMFilterCommon() {
	}
	 
	/**
	 * Default Constructor - Value object which eventually stored as a byte array
	 * @param keyword	Tagged keywords
	 * @param state	Document state
	 * @param team	Tenant or Organization Unit
	 * @param createdBefore	Created Before
	 * @param createdAfter	Created After
	 * @param modifiedBefore	Modified Before
	 * @param modifiedAfter	Modified After
	 */
	public AMFilterCommon(AccessStorable viewAcls, 
		byte[] keyword, byte[] state ,byte[] team,
		long createdBefore, long createdAfter,long modifiedBefore, long modifiedAfter ) {

		boolean hasAcl = ( null != viewAcls);
		boolean hasKeyword = ( null != keyword);
		boolean hasState = ( null != state);
		boolean hasTeam = ( null != team);
		boolean hasCB = ( -1 != createdBefore);
		boolean hasCA = ( -1 != createdAfter);
		boolean hasMB = ( -1 != modifiedBefore);
		boolean hasMA = ( -1 != modifiedAfter);
		
		byte filterFlag = Storable.bitsToByte(new boolean[]{
			hasAcl, hasKeyword, hasState,hasTeam,hasCB,hasCA,hasMB,hasMA});
		
		int totalBytes = 1;
		byte[] aclB = ( hasAcl) ? viewAcls.toBytes() : null;
		if ( hasAcl) totalBytes = totalBytes + aclB.length + 2;
		if ( hasKeyword ) totalBytes = totalBytes + keyword.length + 2;
		if ( hasState ) totalBytes = totalBytes + state.length + 2;
		if ( hasTeam ) totalBytes = totalBytes + team.length + 2;
		if ( hasCB ) totalBytes = totalBytes + 8;
		if ( hasCA ) totalBytes = totalBytes + 8;
		if ( hasMB ) totalBytes = totalBytes + 8;
		if ( hasMA ) totalBytes = totalBytes + 8;
		
		byte[] bytes = new byte[totalBytes];
		int index=0;
		bytes[index++] = filterFlag;
		if ( hasAcl ) index = writeBytes(aclB, bytes, index);
		if ( hasKeyword ) index = writeBytes(keyword, bytes, index);
		if ( hasState ) index = writeBytes(state, bytes, index);
		if ( hasTeam ) index = writeBytes(team, bytes, index);
		if ( hasCB ) index = writeLong(createdBefore, bytes, index);
		if ( hasCA ) index = writeLong(createdAfter, bytes, index);
		if ( hasMB ) index = writeLong(modifiedBefore, bytes, index);
		if ( hasMA ) index = writeLong(modifiedAfter, bytes, index);
		
		this.bytesA = bytes;
	}

	/**
	 * Write the header section
	 * @param out
	 * @throws IOException
	 */
	public void writeHeader(DataOutput out) throws IOException {
		out.writeInt(this.bytesA.length);
		out.write(this.bytesA);
	}
	 
	/**
	 * Read the header section and deserialized the input
	 * @param in
	 * @throws IOException
	 */
	public void readHeader(DataInput in) throws IOException {
		int totalB = in.readInt();
		this.bytesA = new byte[totalB];
		in.readFully(this.bytesA, 0, totalB);
		deserialize();
	}

	/**
	 * Forms the object from the input byte array
	 *
	 */
	public void deserialize() {
		int index=0;
		byte filterFlag = this.bytesA[index++];
		boolean[] filterFlags = Storable.byteToBits(filterFlag);
		byte counter = 0;
		
		if ( filterFlags[counter++]) {
			short len = Storable.getShort(index, this.bytesA);
			index = index + 2;
			this.userAcl = new AccessStorable(this.bytesA, index, len);
			index = index + len;
		}
		
		if ( filterFlags[counter++] ) {
			short len = Storable.getShort(index, this.bytesA);
			this.keyword = new byte[len];
			index = index + 2;
			System.arraycopy(this.bytesA, index, this.keyword, 0, len);
			index = index + len;
		}

		if ( filterFlags[counter++] ) {
			short len = Storable.getShort(index, this.bytesA);
			this.state = new byte[len];
			index = index + 2;
			System.arraycopy(this.bytesA, index, this.state, 0, len);
			index = index + len;
		}

		if ( filterFlags[counter++] ) {
			short len = Storable.getShort(index, this.bytesA);
			this.team = new byte[len];
			index = index + 2;
			System.arraycopy(this.bytesA, index, this.team, 0, len);
			index = index + len;
		}

		if ( filterFlags[counter++]) {
			this.maxCreationDate = Storable.getLong(index, this.bytesA);
			index = index + 8;
		}
		
		if ( filterFlags[counter++]) {
			this.minCreationDate = Storable.getLong(index, this.bytesA);
			index = index + 8;
		}
		
		if ( filterFlags[counter++]) {
			this.maxModificationDate = Storable.getLong(index, this.bytesA);
			index = index + 8;
		}

		if ( filterFlags[counter++]) {
			this.minModificationDate = Storable.getLong(index, this.bytesA);
			index = index + 8;
		}
	}	
	
	/**
	 * Checks for allowed Access. It scans the block size also.
	 * @param value	Input bytes
	 * @param pos	Read start position
	 * @return	-1 if not Allowed else end position of block
	 */
	public int allowAccess( byte[] value, int pos)  {
		boolean isAllowed = false;
		if ( null == value ) isAllowed = true;
		short len = Storable.getShort(pos, value);
		pos = pos + 2;
		if ( ! isAllowed ) {
			AccessStorable docAcls = FilterObjectFactory.getInstance().getStorableAccess();			
			docAcls.reset(value,pos,len);
			isAllowed = checkAccess(docAcls);
			FilterObjectFactory.getInstance().putStorableAccess(docAcls);			
			if ( ! isAllowed ) return -1;
		}
		pos = pos + len;
		len = Storable.getShort(pos, value);
		pos = pos + 2 + len;  //editAclB
		return pos;
	}

	private boolean checkAccess(AccessStorable docAcls) {
		boolean isAllowed = false;
		if ( 0 == docAcls.size() ) isAllowed = true;
		else {
			for (Object docAclO : docAcls) {
				if (compareBytes(0, (byte[]) docAclO, Access.ANY_BYTES)) {
					isAllowed = true;
					break;
				}
				
				if ( null == this.userAcl) return false;
				for (Object userAcl : this.userAcl) {
					if ( compareBytes(0, (byte[]) docAclO, (byte[]) userAcl) ) {
						isAllowed = true;
						break;
					}
				}
				if ( isAllowed ) break;
			}
		}
		return isAllowed;
	}
	
	public static int measureAccess( byte[] value, int pos)  {
		int startPos = pos;
		if ( null == value ) return -1;
		short len = Storable.getShort(pos, value);
		pos = pos + 2 + len;
		len = Storable.getShort(pos, value);		
		pos = pos + 2 + len;  //editAclB
		return (pos - startPos);
	}	
	
	/**
	 * Filter meta fields based on user supplied filtering criteria
	 * @param storedB	Stored bytes
	 * @param pos	The starting position
	 * @return	-1 if not Matched else end position of block
	 */
	public int allowMeta(byte[] storedB, int pos) {
		byte docTypeLen = storedB[pos++];
		pos = pos + docTypeLen;
		
		byte stateLen = storedB[pos++];
		if ( null != state) {
			if ( ! compareBytes(pos, storedB, state) ) return -1;
		}
		pos = pos + stateLen;
		byte tenantLen = storedB[pos++];
		if ( null != team) {
			if ( ! compareBytes(pos, storedB, team) ) return -1;
		}
		pos = pos + tenantLen;
		
		byte geoLen = storedB[pos++];
		pos = pos + geoLen;
		
		byte flag_1B = storedB[pos++];
		boolean[] flag_1 = Storable.byteToBits(flag_1B);
		
		byte flag_2B = storedB[pos++];
		boolean[] flag_2 = Storable.byteToBits(flag_2B);
		
		int bitPos = 0;
		if ( flag_1[bitPos++]) pos = pos+ 4; /** Eastering */
		if ( flag_1[bitPos++]) pos = pos+ 4; /** Northing */
		if ( flag_1[bitPos++]) pos = pos+ 4; /** Weight */
		if ( flag_1[bitPos++]) pos = pos+ 4; /** IP House */
		bitPos = bitPos+ 2; /** Security and Sentiment */
		if ( flag_1[bitPos++]) {  /** Tags Available*/
			short len = Storable.getShort(pos, storedB);
			pos = pos + 2;
			if ( null != this.keyword ) {
				if (this.keyword.length > len ) return -1;
				if ( -1 == indexOf(storedB, pos, pos+len,
					this.keyword, 0, this.keyword.length) ) return -1;
			}
			pos = pos+ len;
		} else {
			if ( null != this.keyword ) return -1; //No tags found
		}
		if ( flag_1[bitPos++]) { //Social text
			short len = Storable.getShort(pos, storedB);
			pos = pos + 2 + len;
		}
		bitPos = 0;
		if (flag_2[bitPos++]) {
			long createdOn = Storable.getLong(pos, storedB);
			pos = pos+ 8;
			if ( -1 != maxCreationDate) {
				if ( maxCreationDate < createdOn) return -1;
			}
			if ( -1 != minCreationDate) {
				if ( minCreationDate > createdOn ) return -1;
			}
		}
		
		if (flag_2[bitPos++]) {
			long modifiedOn = Storable.getLong(pos, storedB);
			pos = pos+ 8;
			if ( -1 != maxModificationDate) {
				if ( maxModificationDate < modifiedOn) return -1;
			}
			if ( -1 != minModificationDate) {
				if ( minModificationDate > modifiedOn ) return -1;
			}
		}
		if (flag_2[bitPos++]) pos = pos+ 8;
		return pos;
	}
	
	public static int measureMeta(byte[] storedB, int pos) {
		
		int startPos = pos;
		
		byte docTypeLen = storedB[pos++];
		pos = pos + docTypeLen;
		
		byte stateLen = storedB[pos++];
		pos = pos + stateLen;
		
		byte tenantLen = storedB[pos++];
		pos = pos + tenantLen;
		
		byte geoLen = storedB[pos++];
		pos = pos + geoLen;

		byte flag_1B = storedB[pos++];
		boolean[] flag_1 = Storable.byteToBits(flag_1B);
		byte flag_2B = storedB[pos++];
		boolean[] flag_2 = Storable.byteToBits(flag_2B);
		int bitPos = 0;
		
		if ( flag_1[bitPos++]) pos = pos+ 4;
		if ( flag_1[bitPos++]) pos = pos+ 4;
		if ( flag_1[bitPos++]) pos = pos+ 4;
		if ( flag_1[bitPos++]) pos = pos+ 4;
		bitPos = bitPos+ 2; /** Security and Sentiment */
		if ( flag_1[bitPos++]) {  /** Tags Available*/
			short len = Storable.getShort(pos, storedB);
			pos = pos + 2 + len;
		}
		if ( flag_1[bitPos++]) { //Social text
			short len = Storable.getShort(pos, storedB);
			pos = pos + 2 + len;
		}
		
		bitPos = 0;
		if (flag_2[bitPos++]) pos = pos+ 8;
		if (flag_2[bitPos++]) pos = pos+ 8;
		if (flag_2[bitPos++]) pos = pos+ 8;
		
		return (pos - startPos);
	}	
	
	private static boolean compareBytes(int offset, 
		byte[] inputBytes, byte[] compareBytes) {
		
		int compareBytesT = compareBytes.length;
		if ( (offset + compareBytesT) > inputBytes.length ) return false;
		if ( compareBytes[0] != inputBytes[offset]) return false;
		if ( compareBytes[compareBytesT - 1] != inputBytes[compareBytesT + offset - 1] ) return false;
		switch (compareBytesT) {
		
		case 3:
			return compareBytes[1] == inputBytes[1 + offset];
		case 4:
			return compareBytes[1] == inputBytes[1 + offset] && 
				compareBytes[2] == inputBytes[2 + offset];
		case 5:
			return compareBytes[1] == inputBytes[1+ offset] && 
				compareBytes[2] == inputBytes[2+ offset] && 
				compareBytes[3] == inputBytes[3+ offset];
		case 6:
			return compareBytes[1] == inputBytes[1+ offset] && 
			compareBytes[3] == inputBytes[3+ offset] && 
			compareBytes[2] == inputBytes[2+ offset] && 
			compareBytes[4] == inputBytes[4+ offset];
		default:
			compareBytesT--;
			for ( int i=0; i< compareBytesT; i++) {
				if ( compareBytes[i] != inputBytes[offset + i]) return false;
			}
		}
		return true;
	}
	
	private int writeLong(long variable, byte[] bytes, int index) {
		System.arraycopy(Storable.putLong(variable),0, bytes, index, 8);
		index = index + 8;
		return index;
	}

	private int writeBytes(byte[] variableB, byte[] bytes, int index) {
		short variableLen = (short) variableB.length;
		bytes[index++] = (byte)(variableLen >> 8 & 0xff);
		bytes[index++] = (byte)(variableLen & 0xff);
		System.arraycopy(variableB, 0, bytes, index, variableLen);
		index = index + variableLen;
		return index;
	}
	
    
    static int indexOf(byte[] source, int startPosition, int endPosition,
    		byte[] target, int targetOffset, int targetCount ) {	
    	
    	byte first  = target[targetOffset];
    	int i = startPosition;
    	startSearchForFirstChar: 
		while (true) {	
			while (i <= endPosition && source[i] != first)i++;
			if (i > endPosition) return -1;
			int j = i + 1;
			int end = j + targetCount - 1;
			int k = targetOffset + 1;
			while (j < end) {
				if (source[j++] != target[k++]) {
					i++;
					continue startSearchForFirstChar;
				}
			}
			return i - startPosition;
		}
	}
}
