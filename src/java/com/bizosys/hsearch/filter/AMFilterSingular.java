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
 * This is a document specific filter gets applied
 * in matching terms. Meta section and Access Control are the 
 * two vital filtering steps after matching keywords.
 * @author karan
 *
 */
public class AMFilterSingular implements Filter {
	private static final char META_BYTE = 'm';
	private static final char ACL_BYTE = 'a';
	private static final char TEASER_BYTE = 't';
	
	
	/**
	 * The implementation class
	 */
	AMFilterCommon amfc = null;
	
	/**
	 * Bytes
	 */
	byte[] bytes = null;

	/**
	 * Default constructor
	 *
	 */
	public AMFilterSingular(){}
	
	/**
	 * Initialized
	 * @param amfc
	 */
	public AMFilterSingular(AMFilterCommon amfc){
		this.amfc = amfc;
	}
	
	/**
	 * Get filtering class
	 * @return	The Filter logic object
	 */
	public AMFilterCommon getFma(){
		return this.amfc;
	}
	
	/**
	 * Set filtering class
	 * @param amfc
	 */
	public void setFma(AMFilterCommon amfc) {
		this.amfc = amfc;
	}

	/**
	 * Not necessary
	 */
	public boolean filterAllRemaining() {
		return false;
	}

	/**
	 *  True to drop this key/value
	 */
	public ReturnCode filterKeyValue(KeyValue kv) {
		if ( ACL_BYTE == kv.getQualifier()[0]) { // Match ACL
			if ( -1 == this.amfc.allowAccess(kv.getValue(),0)) {
				return ReturnCode.NEXT_ROW;
			}
		} else if (META_BYTE == kv.getQualifier()[0]) {
			if ( -1 == this.amfc.allowMeta(kv.getValue(),0)) {
				return ReturnCode.NEXT_ROW;
			}
		}
		return ReturnCode.INCLUDE;
	}

	/**
	 * Not necessary
	 */
	public boolean filterRow() {
		return false;
	}

	/**
	 * Last chance to drop entire row based on the sequence of filterValue() 
	 * calls. Eg: filter a row if it doesn't contain a specified column
	 */
	public void filterRow(List<KeyValue> kvL) {
		if ( null == kvL) return;
		if ( 0 == kvL.size()) return;
		Iterator<KeyValue> kvItr = kvL.iterator();
		for ( int i=0; i< kvL.size(); i++ ) {
			KeyValue kv = kvItr.next();
			byte q = kv.getQualifier()[0];
			if (ACL_BYTE == q) kvItr.remove();
			else if (TEASER_BYTE == q) kvItr.remove();
		}
	}
	
	/**
	 * True to drop this row, if false, we will also call
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
		if ( null == this.amfc ) this.amfc = new AMFilterCommon();
		this.amfc.readHeader(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		if ( null == this.amfc ) this.amfc = new AMFilterCommon();
		amfc.writeHeader(out);
	}
}
