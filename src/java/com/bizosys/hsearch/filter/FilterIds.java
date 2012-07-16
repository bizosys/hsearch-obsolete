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
import java.util.Arrays;

/**
 * Finds out whether a document carries the matching 
 * term hash, document type and term type restricting it inside
 * allowed merged sections (buckets).
 * @author karan
 *
 */
public class FilterIds {
	
	private static final int KEYWORD_BYTES = 5;
	
	/**
	 * Restrict the findings only inside the filtered buckets
	 * After the Top 4 all others are continuous bucket Ids
	 * @param rowKey	The primary key
	 * @param inB	Input Bytes
	 * @return	Is bucket matched?
	 */
	public static final boolean isMatchingBucket(byte[] rowKey, byte[] inB) {
		
		int inBLen = inB.length;
		if ( 6 >= inBLen) return true; //Only Hash + Typecodes

		for ( int i=6; i<inBLen;) {
			if ( inB[i] == rowKey[0] &&
				inB[i+1] == rowKey[1] &&
				inB[i+2] == rowKey[2] &&
				inB[i+3] == rowKey[3] &&
				inB[i+4] == rowKey[4] &&
				inB[i+5] == rowKey[5] &&
				inB[i+6] == rowKey[6] &&
				inB[i+7] == rowKey[7] ) return true;
			i = i+8;	
		}
		return false;
	}
	
	/**
	 * Match the term hash, doc type and term type 
	 * @param storeB	Stored bytes
	 * @param inB	Input Bytes
	 * @return	Matching Term-Lists Byte Array
	 */
	public static final byte[] isMatchingColBytes( byte[] storeB, byte[] inB) {
		if ( null == storeB ) return null;
		if ( null == inB ) return null;
		int inT = inB.length;
		
		int storeL = storeB.length;
		int pos = 0, startPos=0;
		int termsT = 0;
		int codecPos = 0;
		
		int origShift = 0;
		int newShift = 0;
		while ( storeL > pos) { //Loop on keyword hashes
			boolean isMatched =   //Match a Keyword hash 
				storeB[pos] == inB[0] && storeB[pos+1] == inB[1] && 
				storeB[pos+2] == inB[2] && storeB[pos+3] == inB[3];
			pos = pos + 4;
			termsT = (byte) storeB[pos++];
			if ( -1 == termsT) {
				termsT = getInt(pos,storeB );
				pos = pos + 4;
			}

			if ( ! isMatched) { /** Term Hash Not Matched */
				pos = pos + (termsT * KEYWORD_BYTES);
				continue;
			}
			
			boolean[] matchedPositions = new boolean[termsT];
			Arrays.fill(matchedPositions, true);
			codecPos = pos;
			
			/**
			 * Term Hash code has matched.
			 */
			int matched = 0;
			if ( inT > 4 && Byte.MIN_VALUE != inB[4]) { /** Doc Type code match needed*/
				for (int i=0; i<termsT; i++ ) {
					if ( storeB[pos+i] == inB[4] ) { //Any one is matched
						matched++; 
					} else {
						matchedPositions[i] = false;						
					}
				}
			} else {
				matched = termsT;
			}
			
			if ( ! isMatched) { /** Doc Type Has Not Matched */
				pos = pos + (termsT * KEYWORD_BYTES);
				continue;
			}
			
			
			if ( inT > 5 && Byte.MIN_VALUE != inB[5]) { /** Term  Type code match needed*/
				startPos = pos+termsT;
				for (int i=0; i<termsT; i++ ) {
					if ( ! matchedPositions[i] ) continue;
					if ( storeB[startPos+i] != inB[5] ) {
						matchedPositions[i] = false;
						matched--; 
					}
				}
			} 
			
			if ( matched == 0) { /** Term Type Has Not Matched */
				pos = pos + (termsT * KEYWORD_BYTES);
				continue;
			}
			
			/** Keyword, Termtype, Doctype all has Matched */
			byte[] termLstBytes = new byte[matched * KEYWORD_BYTES];
			
			if ( matched == termsT) {
				System.arraycopy(storeB,codecPos, termLstBytes, 0, termLstBytes.length);
				return termLstBytes;
			}
			
			/**
			 * Return selectively
			 */
			for ( int j=0,i=0; i< termsT; i++) {
				if (! matchedPositions[i]) continue;
				origShift = codecPos + i;
				newShift = j;
				termLstBytes[newShift] 			= storeB[origShift]; //Doc Type
				newShift = newShift + matched;
				origShift = origShift + termsT;
				termLstBytes[newShift]	= storeB[origShift]; //term Type
				newShift = newShift + matched;
				origShift = origShift + termsT;
				termLstBytes[newShift]	= storeB[origShift]; //Term weight
				newShift = newShift + matched + j;
				origShift = origShift + termsT + i;
				termLstBytes[newShift]	= storeB[origShift]; //Doc Pos
				termLstBytes[newShift+1]	= storeB[origShift + 1]; //Doc Pos
				j++;
			}
			return termLstBytes;
		}
		return null;
	}
	
	/**
	 * Reads the header section of input data to find total bytes encapsuled
	 * @param in	Input data
	 * @return	Total bytes to be read
	 * @throws IOException
	 */
	public static final int readHeader(DataInput in) throws IOException {
		int T = (in.readByte() << 24 ) + 
		( (in.readByte() & 0xff ) << 16 ) + 
		(  ( in.readByte() & 0xff ) << 8 ) + 
		( in.readByte() & 0xff );
		return T;
	}	
	
	/**
	 * Write the header seciton of supplied header
	 * @param out	Data output
	 * @param BT	Total Bytes
	 * @throws IOException
	 */
	public static final void writeHeader(DataOutput out, int BT) throws IOException {
		out.write(new byte[] { (byte)(BT >> 24),
			(byte)(BT >> 16 ),(byte)(BT >> 8 ), (byte)(BT) });
	}
	
	
	/**
	 * Integer - Byte conversion
	 * @param index	The reading start position
	 * @param inputBytes	Byte Array
	 * @return	The Integer data type
	 */
	public static final int getInt(int index, byte[] inputBytes) {
		
		int intVal = (inputBytes[index] << 24 ) + 
		( (inputBytes[++index] & 0xff ) << 16 ) + 
		(  ( inputBytes[++index] & 0xff ) << 8 ) + 
		( inputBytes[++index] & 0xff );
		return intVal;
	}
	
}
