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
 * It extracts preview details of a single document out of merged bytes 
 * @author karan
 *
 */
public class PreviewFilterMerged implements Filter {
	private static final char SEARCH_FAM = 'S';
	private static final char TEASER_FAM = 'T';

	public static final char META_HEADER_0 = 'm';
	public static final char META_DETAIL_0 = 'n';
	public static final char ACL_HEADER_0 = 'a';
	public static final char ACL_DETAIL_0 = 'b';
	private static final char TEASER_HEADER_0 = 't';
	private static final char TEASER_DETAIL_0 = 'u';
	
	
	private int docSerial = -1;
	byte[] aclH=null, aclD=null, metaH=null, metaD=null, teaserH=null, teaserD=null;
	
	public PreviewFilterMerged(){
	}

	public PreviewFilterMerged(int docSerial){
		this.docSerial = docSerial;
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
		
		if ( -1 == docSerial) return; 
		if ( null == kvL) return;
		if ( 0 == kvL.size()) return;
		
		getExistingBlocks(kvL);

		int aclStartPos=-1,aclLen=-1,metaStartPos=-1,metaLen=-1, teaserStartPos=-1,teaserLen=-1;
		if ( null != aclH) {
			aclStartPos = MergedBlocks.readHeader(aclH, docSerial);
			if ( -1 != aclStartPos) aclLen = AMFilterCommon.measureAccess(aclD, aclStartPos);
		}
		if ( null != metaH) {
			metaStartPos = MergedBlocks.readHeader(metaH, docSerial);
			if ( -1 != metaStartPos) metaLen = AMFilterCommon.measureMeta(metaD, metaStartPos);
		}
		
		if ( null != teaserH) {
			teaserStartPos = MergedBlocks.readHeader(teaserH, docSerial);
			if ( -1 != teaserStartPos) teaserLen = TeaserMarker.measure(teaserD, teaserStartPos);
		}
		
		
		KeyValue prestine = kvL.get(0);
		kvL.clear();
		byte[] r = prestine.getRow();
		if ( aclLen > 0) {
			byte[] data = new byte[aclLen];
			System.arraycopy(aclD, aclStartPos, data, 0, aclLen);
			kvL.add(new KeyValue(r,new byte[]{SEARCH_FAM},new byte[]{ACL_DETAIL_0},data));
		}
		if ( metaLen > 0) {
			byte[] data = new byte[metaLen];
			System.arraycopy(metaD, metaStartPos, data, 0, metaLen);
			kvL.add(new KeyValue(r,new byte[]{SEARCH_FAM},new byte[]{META_DETAIL_0},data));
		}
		if ( teaserLen > 0) {
			byte[] data = new byte[teaserLen];
			System.arraycopy(teaserD, teaserStartPos, data, 0, teaserLen);
			kvL.add(new KeyValue(r,new byte[]{TEASER_FAM},new byte[]{TEASER_DETAIL_0},data));
		}
	}

	private void getExistingBlocks(List<KeyValue> kvL) {
		for (KeyValue kv : kvL) {
			switch ( kv.getQualifier()[0] ) {
				case META_HEADER_0:
					this.metaH = kv.getValue();
					break;
				case META_DETAIL_0:
					this.metaD = kv.getValue();
					break;
				case ACL_HEADER_0:
					this.aclH = kv.getValue();
					break;
				case ACL_DETAIL_0:
					this.aclD = kv.getValue();
					break;
				case TEASER_HEADER_0:
					this.teaserH = kv.getValue();
					break;
				case TEASER_DETAIL_0:
					this.teaserD = kv.getValue();
					break;
				default:
					System.err.println("Unknown Column :" + new String(kv.getQualifier()));
					break;
			}
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
		this.docSerial = in.readInt();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.docSerial);
	}
}
