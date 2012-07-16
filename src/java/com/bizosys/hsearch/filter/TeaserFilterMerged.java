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
 * Sending the complete document over the wire may Jam the network on a 
 * heavy concurrent user base. This filter ensures sending the most
 * relevant section only. It also uses multiple Region servers to create
 * the teasers to serve a search request.
 * @author karan
 */
public class TeaserFilterMerged implements Filter {
	private static final char TEASER_HEADER = 't';
	private static final char TEASER_DETAIL = 'u';
	
	private static final byte[] TEASER_HEADER_BYTES = "t".getBytes();
	private static final byte[] TEASER_DETAIL_BYTES = "u".getBytes();

	/**
	 * Default teaser section length
	 */
	short cutLength = 360;
	
	/**
	 * Searched words
	 */
	byte[][] bWords = null;
	
	/**
	 * Only extract these document serial numbers
	 */
	private int[] docSerials = null;	

	/**
	 * Default constructor
	 *
	 */
	public TeaserFilterMerged(){}
	
	/**
	 * Constructor
	 * @param bWords	Searched words
	 * @param cutLength	Teaser section length
	 */
	public TeaserFilterMerged(byte[][] bWords, short cutLength){
		this.bWords = bWords;
		this.cutLength = cutLength; 
	}
	
	public void setDocSerials(int[] docSerials) {
		this.docSerials = docSerials;
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
		if ( null == kvL || null == docSerials ) return;
		if ( 0 == kvL.size()) return;
		
		MergedBlocks.Block teaserBlocks = new MergedBlocks.Block();
		KeyValue prestine = getExistingBlocks(kvL, teaserBlocks);
		if ( null == prestine) return;
		byte[] header = teaserBlocks.header;
		if ( null == header) return; 
		
		byte[] data = teaserBlocks.data;
		if ( null == data) return; 
		
		int totalDocs = docSerials.length;
		int start=0;

		List<TeaserMarker> markings = FilterObjectFactory.getInstance().getTeaserMarker();
		TeaserFilterCommon tf = new TeaserFilterCommon(bWords);
		for ( int i=0; i< totalDocs; i++) {
			int docSerial = docSerials[i];
			start = MergedBlocks.readHeader(header, docSerial);
			if ( -1 == start) continue;
			TeaserMarker marker = new TeaserMarker(docSerial,data,start,tf,cutLength);
			markings.add(marker);
		}
		
		if ( 0 == markings.size()) {
			kvL.clear();
			FilterObjectFactory.getInstance().putTeaserMarker(markings);
			return;
		}
		
		int dataLenTeaser = 0;
		for (TeaserMarker marker : markings) {
			dataLenTeaser = dataLenTeaser + marker.getNewSize();
		}
		
		byte[] teaserHeader = new byte[markings.size() * 2];
		byte[] teaserData = new byte[dataLenTeaser];
		
		int teaserDataPos = 0, teaserHeaderPos=0; 
		for (TeaserMarker marker : markings) {
			teaserHeader[teaserHeaderPos++] = (byte)(marker.serial >> 8 & 0xff); 
			teaserHeader[teaserHeaderPos++] = (byte)(marker.serial & 0xff); 
			teaserDataPos = marker.extract(teaserBlocks.data, teaserData , teaserDataPos);
		}

		FilterObjectFactory.getInstance().putTeaserMarker(markings);

		kvL.clear();
		byte[] r = prestine.getRow();
		byte[] f = prestine.getFamily();
		kvL.add(new KeyValue(r,f,TEASER_HEADER_BYTES,teaserHeader));
		kvL.add(new KeyValue(r,f,TEASER_DETAIL_BYTES,teaserData));
		
	}
	
	private KeyValue getExistingBlocks(List<KeyValue> kvL, MergedBlocks.Block teaserBlocks) {
		KeyValue prestine = null;  
		for (KeyValue kv : kvL) {
			if ( null == prestine) {
				prestine = new KeyValue(kv.getRow(),kv.getFamily(), kv.getQualifier());
			}
			
			switch ( kv.getQualifier()[0] ) {
				case TEASER_HEADER:
					teaserBlocks.header = kv.getValue();
					break;
				case TEASER_DETAIL:
					teaserBlocks.data = kv.getValue();
					break;
				default:
					System.err.println("\n\n Error : Unknown Column :" + new String(kv.getQualifier()));
					break;
			}
		}
		return prestine;
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
		int totalDocs = in.readInt();
		docSerials = new int[totalDocs];
		for ( int i=0; i<totalDocs; i++ ) {
			docSerials[i] = in.readInt();
		}
		
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
		int totalDocs = ( null == this.docSerials) ? 0 : docSerials.length;
		out.writeInt(totalDocs);
		if ( totalDocs > 0) {
			for (int serial : docSerials) {
				out.writeInt(serial);
			}
		}
		
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
	
	public TeaserFilterMerged clone() {
		TeaserFilterMerged another = new TeaserFilterMerged();
		another.bWords =  this.bWords;
		another.cutLength = this.cutLength;
		return another;
	}
}
