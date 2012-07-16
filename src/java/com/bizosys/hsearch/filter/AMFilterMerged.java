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
 * This is a document specific filter gets applied
 * in matching terms. Meta section and Access Control are the 
 * two vital filtering steps after matching keywords.
 * @author karan
 *
 */
public class AMFilterMerged implements Filter {
	public static final char META_HEADER_0 = 'm';
	private static final byte[] META_HEADER_B = new byte[]{META_HEADER_0};
	public static final char META_DETAIL_0 = 'n';
	private static final byte[] META_DETAIL_B = new byte[]{META_DETAIL_0};
	public static final char ACL_HEADER_0 = 'a';
	public static final char ACL_DETAIL_0 = 'b';

	private int[] docSerials = null;
	
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
	public AMFilterMerged(){
	}
	
	/**
	 * Initialized
	 * @param amfc
	 */
	public AMFilterMerged(AMFilterCommon amfc){
		this.amfc = amfc;
	}
	
	public void setDocSerials(int[] docSerials){
		this.docSerials = docSerials;
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
		
		if ( null == docSerials ) return; 
		MergedBlocks.Block metaBlocks = new MergedBlocks.Block();
		MergedBlocks.Block aclBlocks = new MergedBlocks.Block();
		KeyValue prestine = getExistingBlocks(kvL, metaBlocks, aclBlocks);
		if ( null == prestine) return;
		
		int totalDocs = docSerials.length;
		List<AMMarker> markings = FilterObjectFactory.getInstance().getAMMarkers(); 		
		mark(metaBlocks, aclBlocks, totalDocs, markings);

		kvL.clear();
		
		int dataLenMeta = 0;
		for (AMMarker marker : markings) {
			dataLenMeta = dataLenMeta + (marker.metaEnd - marker.metaStart);
		}
		
		byte[] metaHeader = new byte[markings.size() * 2];
		byte[] metaData = new byte[dataLenMeta];
		
		int metaDataPos = 0, len = 0, metaHeaderPos = 0; 
		FilterObjectFactory  fof = FilterObjectFactory.getInstance();
		
		for (AMMarker marker : markings) {
			metaHeader[metaHeaderPos++] = (byte)(marker.serial >> 8 & 0xff); 
			metaHeader[metaHeaderPos++] = (byte)(marker.serial & 0xff); 
			len = (marker.metaEnd - marker.metaStart);
			System.arraycopy(metaBlocks.data, marker.metaStart, metaData, metaDataPos, len);
			metaDataPos = metaDataPos + len;
			fof.putOneAMMarker(marker);
		}
		
		fof.putAMMarkers(markings);
		
		byte[] r = prestine.getRow();
		byte[] f = prestine.getFamily();
		kvL.add(new KeyValue(r,f,META_HEADER_B,metaHeader));
		kvL.add(new KeyValue(r,f,META_DETAIL_B,metaData));
	}

	private void mark(MergedBlocks.Block metaBlocks, MergedBlocks.Block aclBlocks, 
		int totalDocs, List<AMMarker> markings) {
		
		int aclStartPos, aclEndPos, metaStartPos, metaEndPos, docSerial;
		FilterObjectFactory  fof = FilterObjectFactory.getInstance();
		
		for ( int i=0; i< totalDocs; i++) {
			docSerial = docSerials[i];
			aclStartPos=0; aclEndPos=0 ; metaStartPos=0 ; metaEndPos=0;			

			if ( null != aclBlocks.header) {
				aclStartPos = MergedBlocks.readHeader(aclBlocks.header, docSerial);
				if ( -1 != aclStartPos) {
					aclEndPos = this.amfc.allowAccess(aclBlocks.data, aclStartPos);
					if ( -1 == aclEndPos ) continue;
				}
			}
				
			if ( null != metaBlocks.header) {
				metaStartPos = MergedBlocks.readHeader(metaBlocks.header, docSerial);
				if ( -1 == metaStartPos) continue;
				metaEndPos = this.amfc.allowMeta(metaBlocks.data, metaStartPos);
				if ( -1 == metaEndPos) continue;
			}
			
			//ACL as well as META is OK
			AMMarker aMarker = fof.getOneAMMarker();
			aMarker.set(docSerial,aclStartPos,aclEndPos,metaStartPos,metaEndPos);
			markings.add(aMarker);
		}
	}

	private KeyValue getExistingBlocks(List<KeyValue> kvL, MergedBlocks.Block metaBlocks, MergedBlocks.Block aclBlocks) {
		KeyValue prestine = null;  
		for (KeyValue kv : kvL) {
			
			if ( null == prestine) {
				prestine = new KeyValue(kv.getRow(),kv.getFamily(), kv.getQualifier());
			}
			
			switch ( kv.getQualifier()[0] ) {
				case META_HEADER_0:
					metaBlocks.header = kv.getValue();
					break;
				case META_DETAIL_0:
					metaBlocks.data = kv.getValue();
					break;
				case ACL_HEADER_0:
					aclBlocks.header = kv.getValue();
					break;
				case ACL_DETAIL_0:
					aclBlocks.data = kv.getValue();
					break;
				default:
					System.err.println("Unknown Column :" + new String(kv.getQualifier()));
					break;
			}
		}
		return prestine;
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
		int totalDocs = in.readInt();
		docSerials = new int[totalDocs];
		for ( int i=0; i<totalDocs; i++ ) {
			docSerials[i] = in.readInt();
		}
		if ( null == this.amfc ) this.amfc = new AMFilterCommon();
		this.amfc.readHeader(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		int totalDocs = ( null == this.docSerials) ? 0 : docSerials.length;
		out.writeInt(totalDocs);
		if ( totalDocs > 0) {
			for (int serial : docSerials) {
				out.writeInt(serial);
			}
		}

		if ( null == this.amfc ) this.amfc = new AMFilterCommon();
		amfc.writeHeader(out);
	}
}
