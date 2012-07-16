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
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;

/**
 * Sending the complete document over the wire may Jam the network on a 
 * heavy concurrent user base. This filter ensures sending the most
 * relevant section only. It also uses multiple Region servers to create
 * the teasers to serve a search request.
 * @author karan
 */
public class TeaserFilterSingular implements Filter {
	private static final byte TEASER_BYTE = "T".getBytes()[0];
	
	/**
	 * Default teaser section length
	 */
	short cutLength = 360;
	
	/**
	 * Searched words
	 */
	byte[][] bWords = null;

	/**
	 * Default constructor
	 *
	 */
	public TeaserFilterSingular(){}
	
	/**
	 * Constructor
	 * @param bWords	Searched words
	 * @param cutLength	Teaser section length
	 */
	public TeaserFilterSingular(byte[][] bWords, short cutLength){
		this.bWords = bWords;
		this.cutLength = cutLength; 
	}
	
	public boolean filterAllRemaining() {
		return false;
	}

	public boolean filterRow() {
		return false;
	}

	/**
	 * last chance to drop entire row based on the sequence of filterValue() 
	 * calls. Eg: filter a row if it doesn't contain a specified column
	 */
	public void filterRow(List<KeyValue> kvL) {
		if ( null == kvL) return;
		int kvT = kvL.size();
		if ( 0 == kvT) return;
		
		KeyValue kv = null;
		
		Iterator<KeyValue> kvItr = kvL.iterator();
		TeaserMarker marker = null;
		byte[] source = null;
		for ( int i=0; i< kvT; i++ ) {
			kv = kvItr.next();
			if (TEASER_BYTE == kv.getFamily()[0]) { //Read the skip sections
				TeaserFilterCommon tfc = new TeaserFilterCommon(bWords);
				source = kv.getValue();
				marker = new TeaserMarker(1,source,0,tfc,cutLength);
				break;
			}
		}
		byte[] dest = new byte[marker.getNewSize()];
		marker.extract(source, dest, 0);
		kvL.add(new KeyValue(kv.getRow(), kv.getFamily(), kv.getQualifier(), dest) );
	}
	
	/**
	 * true to drop this row, if false, we will also call
	 */
	public boolean filterRowKey(byte[] rowKey, int offset, int length) {
		return false;
	}
	
	public KeyValue getNextKeyHint(KeyValue arg0) {
		return null;
	}
	
	public boolean hasFilterRow() {
		return true;
	}
	
	public void reset() {
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.cutLength = in.readShort();
		int len = in.readByte();
		int index = 1;
		this.bWords = new byte[len][];
		
		for ( int i=0; i<len; i++ ) {
			int wLen = in.readByte() ;
			index++;
			this.bWords[i] = new byte[wLen];
			in.readFully(this.bWords[i], 0, wLen);
			index = index + wLen;
		}
		
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeShort(cutLength);
		out.writeByte(bWords.length);
		for ( int i=0; i<bWords.length; i++ ) {
			out.writeByte(bWords[i].length);
			out.write(bWords[i]);
		}
	}

	public ReturnCode filterKeyValue(KeyValue arg0) {
		return ReturnCode.INCLUDE;
	}
}
