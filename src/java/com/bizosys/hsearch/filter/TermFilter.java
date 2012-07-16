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
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;

/**
 * Finds the documents containing the search terms. 
 * It operates on the hash codec of the term.
 * @author karan
 *
 */
public class TermFilter implements Filter {
	/**
	 * All serialized bytes (Document Type, Term Type and others)
	 */
	public byte[]  B;
	
	/**
	 * Hash code bytes
	 */
	public byte[]  H;
	
	/**
	 * Matched term list bytes
	 */
	private byte[] matchedTLBytes = null;
	
	private byte family;
	private byte name;

	/**
	 * Default constructor
	 * DON't USE THIS
	 */
	public TermFilter(){}
	
	/**
	 * Constructor
	 * @param bytes	Serialized bytes
	 */
	public TermFilter( byte[] bytes){
		this.B=bytes;
		this.H = new byte[]{B[0],B[1],B[2],B[3]};
	}
	
	/**
	 * Add the family and column name
	 * @param family
	 * @param colName
	 */
	public void addColumn(byte[] family, byte[] colName) {
		this.family = family[0];
		this.name = colName[0];
	}

	public boolean filterAllRemaining() {
		return false;
	}

	/**
	 *  true to drop this key/value
	 */
	public ReturnCode filterKeyValue(KeyValue kv) {
		matchedTLBytes = null;
		boolean isMatched = FilterIds.isMatchingBucket(kv.getRow(),B);
		if ( isMatched ) {
			if ( kv.getFamily()[0] != family) return ReturnCode.NEXT_COL;
			if ( kv.getQualifier()[0] != name) return ReturnCode.NEXT_COL;
			matchedTLBytes = FilterIds.isMatchingColBytes(kv.getValue(), B);
			isMatched = ( null != matchedTLBytes);
			if (isMatched ) return ReturnCode.INCLUDE;
			return ReturnCode.NEXT_COL;
		}
		return ReturnCode.NEXT_ROW;
	}

	public boolean filterRow() {
		return false;
	}

	/**
	 * last chance to drop entire row based on the sequence of filterValue() 
	 * calls. Eg: filter a row if it doesn't contain a specified column
	 */
	public void filterRow(List<KeyValue> kvL) {
		if ( null == matchedTLBytes) return;
		if ( null == kvL) return;
		if ( 0 == kvL.size()) return;
		KeyValue kv = kvL.get(0);
		kvL.clear();
		kvL.add(new KeyValue(kv.getRow(),
			kv.getFamily(), kv.getQualifier(), matchedTLBytes));
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
		int T = FilterIds.readHeader(in);
		this.B = new byte[T];
		in.readFully(this.B, 0, T);
		this.family = in.readByte();
		this.name = in.readByte();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		int BT = B.length;
		FilterIds.writeHeader(out, BT);
		out.write(B);
		out.write(this.family);
		out.write(this.name);
	}
}
